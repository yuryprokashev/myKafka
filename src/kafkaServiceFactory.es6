/**
 *Created by py on 14/11/2016
 */
"use strict";

const guid = require('./guid');
module.exports = (kafkaBus) =>{

    let kafkaService = {};

    kafkaService.awaitReplyCache = new Map();

    kafkaService.send = (topic, signRequest, message)=>{
        if(signRequest === true) {
            message.id = guid();
            kafkaService.awaitReplyCache.set(message.id, message); // consider, kafkaService always wants reply for each message it sends
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
                console.log(`message sent ${JSON.stringify(message.response)}`);
                console.log('-------------------');
            }
        };
        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{topic: topic, messages: JSON.stringify(message)}], onProducerSent);
    };

    kafkaService.subscribe = (topic, isSignedRequest, callback) => {

        let onTopicsAdded = (err, added) => {
            if(err){
                console.log('consumer failed to add topics');
                console.log(err);
                console.log('-------------');
            }
        };
        let onConsumerMessage = (message) => {
            if(isSignedRequest === true) {
                console.log(`topic ${topic} isSignedRequest = true`);
                let messageId = kafkaService.extractId(message);
                if(message.topic === topic && kafkaService.awaitReplyCache.has(messageId)){
                    console.log(`message.id ${messageId} match for ${topic} -> now executing callback ${callback.name}`);
                    console.log('consumer received message');
                    console.log(message);
                    callback(message);
                    kafkaService.awaitReplyCache.delete(messageId);
                }
            }
            else {
                console.log(`topic ${topic} isSignedRequest = false`);
                if(message.topic === topic) {
                    console.log('consumer received message');
                    console.log(`topic match ${topic} -> now executing callback`);
                    console.log(message);
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
        if(context !== null) {
            id = context.id;
            return id;
        }
        else {
            return null;
        }

    };

    kafkaService.extractContext = kafkaMessage => {
        let context, topic;

        context = JSON.parse(kafkaMessage.value);

        // topic = kafkaService.makeResponseTopic(kafkaMessage);

        if(context === undefined || context === null) {
            // let newContext = {};
            // newContext.response = {error: 'arrived context is empty'};
            // kafkaService.send(topic, false, newContext);
            return null;
        }
        return context;
    };

    kafkaService.extractQuery = kafkaMessage => {
        let query, topic;

        query = JSON.parse(kafkaMessage.value).request.query;
        // topic = kafkaService.makeResponseTopic(kafkaMessage);
        if(query === undefined || query === null) {
            // let context;
            // context = kafkaService.extractContext(kafkaMessage);
            // context.response = {error: 'query is empty'};
            // kafkaService.send(topic, false, context);
            return null;
        }
        else {
            return query;
        }
    };

    kafkaService.extractWriteData = kafkaMessage => {
        let writeData, topic;

        writeData = JSON.parse(kafkaMessage.value).request.writeData;
        // topic = kafkaService.makeResponseTopic(kafkaMessage);
        if(writeData === undefined || writeData === null) {
            // let context;
            // context = kafkaService.extractContext(kafkaMessage);
            // context.response = {error: 'writeData is empty'};
            // kafkaService.send(topic, false, context);
            return null;
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