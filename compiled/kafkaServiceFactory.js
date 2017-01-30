/**
 *Created by py on 14/11/2016
 */
"use strict";

var guid = require('./guid');
module.exports = function (kafkaBus) {

    var kafkaService = {};

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

    kafkaService.send = function (topic, message) {
        message.id = guid();
        console.log(message.id);
        kafkaService.awaitReplyCache.set(message.id, message); // consider, kafkaService always wants reply for each message it sends

        var onProducerError = function onProducerError(err) {
            console.log('producer error');
            console.log(err);
            console.log('--------------');
        };
        var onProducerSent = function onProducerSent(err, data) {
            if (err) {
                console.log('producer sent error');
                console.log(err);
                console.log('-------------------');
            }
            if (data) {
                console.log('producer sent success');
                console.log(data);
                // console.log(`message sent ${JSON.stringify(message)}`);
                console.log('-------------------');
            }
        };
        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{ topic: topic, messages: JSON.stringify(message) }], onProducerSent);
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

    kafkaService.subscribe = function (topic, signRequest, callback) {

        var onTopicsAdded = function onTopicsAdded(err, added) {
            if (err) {
                console.log('consumer failed to add topics');
                console.log(err);
                console.log('-------------');
            }
        };
        var onConsumerMessage = function onConsumerMessage(message) {
            if (signRequest === true) {
                console.log('signRequest = true');
                var messageId = kafkaService.extractId(message);
                console.log('message.id = ' + messageId);
                if (message.topic === topic && kafkaService.awaitReplyCache.has(messageId)) {
                    console.log('now executing callback');
                    callback(message);
                    kafkaService.awaitReplyCache.delete(messageId);
                }
            } else {
                console.log('signRequest = false');
                if (message.topic === topic) {
                    console.log('now executing callback');
                    callback(message);
                }
            }
        };
        var onConsumerError = function onConsumerError(err) {
            console.log('consumer default error');
            console.log(err);
            console.log('-------------');
        };
        var topics = function (qty) {
            var t = [];
            for (var i = 0; i < qty; i++) {
                t.push({ topic: topic, partition: i });
            }
            return t;
        }(1);
        kafkaBus.consumer.addTopics(topics, onTopicsAdded);
        kafkaBus.consumer.on('message', onConsumerMessage);
        kafkaBus.consumer.on('error', onConsumerError);
    };

    kafkaService.extractId = function (kafkaMessage) {
        var context = void 0,
            id = void 0;
        context = kafkaService.extractContext(kafkaMessage);
        id = context.id;
        return id;
    };

    kafkaService.extractContext = function (kafkaMessage) {
        var context = void 0;
        context = JSON.parse(kafkaMessage.value);
        // console.log(kafkaMessage);
        if (context === undefined || context === null) {
            var newContext = {};
            newContext.response = { error: 'arrived context is empty' };
            kafkaService.send(kafkaService.makeResponseTopic(kafkaMessage), newContext);
        }
        return context;
    };

    kafkaService.extractQuery = function (kafkaMessage) {
        var query = JSON.parse(kafkaMessage.value).request.query;
        if (query === undefined || query === null) {
            var context = void 0;
            context = kafkaService.extractContext(kafkaMessage);
            context.response = { error: 'query is empty' };
            kafkaService.send(kafkaService.makeResponseTopic(kafkaMessage), context);
        } else {
            return query;
        }
    };

    kafkaService.extractWriteData = function (kafkaMessage) {
        var profile = JSON.parse(kafkaMessage.value).request.writeData;
        if (profile === undefined || profile === null) {
            var context = void 0;
            context = kafkaService.extractContext(kafkaMessage);
            context.response = { error: 'profile is empty' };
            kafkaService.send(kafkaService.makeResponseTopic(kafkaMessage), context);
        } else {
            return profile;
        }
    };

    kafkaService.makeResponseTopic = function (kafkaMessage) {
        var re = /-request/;
        return kafkaMessage.topic.replace(re, '-response');
    };

    return kafkaService;
};
