/**
 *Created by py on 14/11/2016
 */
"use strict";
module.exports = (kafkaBus) =>{

    let kafkaService = {};

    kafkaService.send = (topic, message)=>{
        let onProducerError = (err) => {
            console.log('producer error');
            console.log(err);
            console.log('--------------');
        };
        let onProducerSent = (err, data) => {
            if(err){
                console.log('producer sent error');
                console.log(err);
                console.log('-------------------');
            }
            if(data){
                console.log('producer sent success');
                console.log(data);
                console.log('-------------------');
            }
        };
        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{topic: topic, messages: JSON.stringify(message)}], onProducerSent);
    };

    kafkaService.subscribe = (topic, callback) => {
        let onTopicsAdded = (err, added) => {
            if(err){
                console.log('consumer failed to add topics');
                console.log(err);
                console.log('-------------');
            }
        };
        let onConsumerMessage = (message) => {
            if(message.topic === topic){
                callback(message);
            }
        };
        let onConsumerError = (err) => {
            console.log('consumer default error');
            console.log(err);
            console.log('-------------');
        };
        let topics = (function(qty){
            let t = [];
            for(let i = 0; i < qty; i++){
                t.push({topic: topic, partition: i});
            }
            return t;
        })(1);
        kafkaBus.consumer.addTopics(topics, onTopicsAdded);
        kafkaBus.consumer.on('message', onConsumerMessage);
        kafkaBus.consumer.on('error', onConsumerError);
    };

    kafkaService.extractContext = kafkaMessage => {
        let context;
        context = JSON.parse(kafkaMessage.value);
        if(context === undefined) {
            let newContext = {};
            newContext.response = {error: 'arrived context is empty'};
            kafkaService.send(kafkaService.makeResponseTopic(kafkaMessage), newContext);
        }
        return context;
    };

    kafkaService.extractQuery = kafkaMessage => {
        let query = JSON.parse(kafkaMessage.value).request.query;
        if(query === undefined || query === null) {
            let context;
            context = kafkaService.extractContext(kafkaMessage);
            context.response = {error: 'query is empty'};
            kafkaService.send(kafkaService.makeResponseTopic(kafkaMessage), context);
        }
        else {
            return query;
        }
    };

    kafkaService.extractWriteData = kafkaMessage => {
        let profile = JSON.parse(kafkaMessage.value).request.writeData;
        if(profile === undefined || profile === null) {
            let context;
            context = kafkaService.extractContext(kafkaMessage);
            context.response = {error: 'profile is empty'};
            kafkaService.send(kafkaService.makeResponseTopic(kafkaMessage), context);
        }
        else {
            return profile;
        }
    };

    kafkaService.makeResponseTopic = kafkaMessage => {
        let re = /-request/;
        return kafkaMessage.topic.replace(re, '-response');
    };

    return kafkaService;
};