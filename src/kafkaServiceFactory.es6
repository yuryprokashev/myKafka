/**
 *Created by py on 14/11/2016
 */
"use strict";

const guid = require('./guid');
module.exports = (kafkaBus) =>{

    let kafkaService = {};

    kafkaService.awaitReplyCache = new Map();

    // kafkaService.send = (topic, message)=>{
    //
    //     let onProducerError = (err) => {
    //         console.log('producer error');
    //         console.log(err);
    //         console.log('--------------');
    //     };
    //     let onProducerSent = (err, data) => {
    //         if(err){
    //             console.log('producer sent error');
    //             console.log(err);
    //             console.log('-------------------');
    //         }
    //         if(data){
    //             console.log('producer sent success');
    //             console.log(data);
    //             // console.log(`message sent ${JSON.stringify(message)}`);
    //             console.log('-------------------');
    //         }
    //     };
    //     kafkaBus.producer.on('error', onProducerError);
    //     kafkaBus.producer.send([{topic: topic, messages: JSON.stringify(message)}], onProducerSent);
    // };

    kafkaService.send = (topic, message)=>{
        message.id = guid();
        kafkaService.awaitReplyCache.set(message.id, message); // consider, kafkaService always wants reply for each message it sends

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
                // console.log(`message sent ${JSON.stringify(message)}`);
                console.log('-------------------');
            }
        };
        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{topic: topic, messages: JSON.stringify(message)}], onProducerSent);
    };



    // kafkaService.subscribe = (topic, callback) => {
    //     let onTopicsAdded = (err, added) => {
    //         if(err){
    //             console.log('consumer failed to add topics');
    //             console.log(err);
    //             console.log('-------------');
    //         }
    //     };
    //     let onConsumerMessage = (message) => {
    //         if(message.topic === topic){
    //             callback(message);
    //         }
    //     };
    //     let onConsumerError = (err) => {
    //         console.log('consumer default error');
    //         console.log(err);
    //         console.log('-------------');
    //     };
    //     let topics = (function(qty){
    //         let t = [];
    //         for(let i = 0; i < qty; i++){
    //             t.push({topic: topic, partition: i});
    //         }
    //         return t;
    //     })(1);
    //     kafkaBus.consumer.addTopics(topics, onTopicsAdded);
    //     kafkaBus.consumer.on('message', onConsumerMessage);
    //     kafkaBus.consumer.on('error', onConsumerError);
    // };

    kafkaService.subscribe = (topic, signRequest, callback) => {

        let onTopicsAdded = (err, added) => {
            if(err){
                console.log('consumer failed to add topics');
                console.log(err);
                console.log('-------------');
            }
        };
        let onConsumerMessage = (message) => {
            if(signRequest === true) {
                console.log('signRequest = true');
                let messageId = kafkaService.extractId(message);
                console.log(`message.id = ${messageId}`);
                if(message.topic === topic && kafkaService.awaitReplyCache.has(messageId)){
                    console.log('now executing callback');
                    callback(message);
                    kafkaService.awaitReplyCache.delete(messageId);
                }
            }
            else {
                console.log('signRequest = false');
                if(message.topic === topic) {
                    console.log('now executing callback');
                    callback(message);
                }
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

    kafkaService.extractId = kafkaMessage => {
        let context, id;
        context = kafkaService.extractContext(kafkaMessage);
        id = context.id;
        return id;
    };

    kafkaService.extractContext = kafkaMessage => {
        let context;
        context = JSON.parse(kafkaMessage.value);
        // console.log(kafkaMessage);
        if(context === undefined || context === null) {
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