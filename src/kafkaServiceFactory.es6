/**
 *Created by py on 14/11/2016
 */
"use strict";

const guid = require('./guid');
module.exports = (kafkaBus) =>{

    let kafkaService = {};

    kafkaService.send = (topic, signature, message) => {
        let args = [...arguments];

        if(args.length === 2) {
            message = signature;
            signature = undefined;
        }

        if(signature !== undefined) {
            message.id = signature;
        }
        // console.log(message.id);
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
                console.log(`message.response sent`);
                console.log(message.response);
                console.log('-------------------');
            }
        };
        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{topic: topic, messages: JSON.stringify(message)}], onProducerSent);
    };

    kafkaService.subscribe = (topic, signature, callback) => {

        let args = [...arguments];

        if(args.length === 2) {
            callback = signature;
            signature = undefined;
        }

        let onTopicsAdded = (err, added) => {
            if(err){
                console.log('consumer failed to add topics');
                console.log(err);
                console.log('-------------');
            }
        };

        let onConsumerMessage = message => {
            let messageSignature;
            messageSignature = kafkaService.extractId(message);

            if(messageSignature.error === undefined) {
                if(message.topic === topic) {
                    if(signature !== undefined && signature === messageSignature) {
                        callback(message);
                    }
                    else if(signature === undefined) {
                        callback(message);
                    }
                    else {
                        console.error('message arrived, but no callback executed');
                    }
                }
            }
            else {
                console.log(messageSignature);
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
        if(context.error === undefined) {
            id = context.id;
            return id;
        }
        else {
            return {error: 'unable to extract id, kafkaMessage has no context'};
        }

    };

    kafkaService.extractContext = kafkaMessage => {
        let context;

        context = JSON.parse(kafkaMessage.value);

        if(context === undefined || context === null) {
            return {error: 'kafkaMessage has no value'};
        }
        return context;
    };

    kafkaService.extractQuery = kafkaMessage => {
        let query;

        query = JSON.parse(kafkaMessage.value).request.query;
        if(query === undefined || query === null) {
            return {error: 'kafkaMessage has no query'};
        }
        else {
            return query;
        }
    };

    kafkaService.extractWriteData = kafkaMessage => {
        let writeData;

        writeData = JSON.parse(kafkaMessage.value).request.writeData;
        if(writeData === undefined || writeData === null) {
            return {error: 'kafkaMessage has no writeData'};
        }
        else {
            return writeData;
        }
    };

    kafkaService.makeResponseTopic = kafkaMessage => {
        let re = /-request/;
        return kafkaMessage.topic.replace(re, '-response');
    };

    return kafkaService;
};