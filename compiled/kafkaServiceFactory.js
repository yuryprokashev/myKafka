/**
 *Created by py on 14/11/2016
 */
"use strict";

var guid = require('./guid');
module.exports = function (kafkaBus) {

    var kafkaService = {};

    kafkaService.awaitReplyCache = new Map();

    kafkaService.send = function (topic, signRequest, message) {
        if (signRequest === true) {
            message.id = guid();
            kafkaService.awaitReplyCache.set(message.id, message); // consider, kafkaService always wants reply for each message it sends
        }
        // console.log(message.id);
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
                console.log('message sent ' + JSON.stringify(message.response));
                console.log('-------------------');
            }
        };
        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{ topic: topic, messages: JSON.stringify(message) }], onProducerSent);
    };

    kafkaService.subscribe = function (topic, isSignedRequest, callback) {

        var onTopicsAdded = function onTopicsAdded(err, added) {
            if (err) {
                console.log('consumer failed to add topics');
                console.log(err);
                console.log('-------------');
            }
        };
        var onConsumerMessage = function onConsumerMessage(message) {
            if (isSignedRequest === true) {
                console.log('topic ' + topic + ' isSignedRequest = true');
                var messageId = kafkaService.extractId(message);
                if (message.topic === topic && kafkaService.awaitReplyCache.has(messageId)) {
                    console.log('message.id ' + messageId + ' match for ' + topic + ' -> now executing callback ' + callback.name);
                    console.log('consumer received message');
                    console.log(message);
                    callback(message);
                    kafkaService.awaitReplyCache.delete(messageId);
                }
            } else {
                console.log('topic ' + topic + ' isSignedRequest = false');
                if (message.topic === topic) {
                    console.log('consumer received message');
                    console.log('topic match ' + topic + ' -> now executing callback');
                    console.log(message);
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
        var context = void 0,
            topic = void 0;

        context = JSON.parse(kafkaMessage.value);

        topic = kafkaService.makeResponseTopic(kafkaMessage);

        if (context === undefined || context === null) {
            var newContext = {};
            newContext.response = { error: 'arrived context is empty' };
            kafkaService.send(topic, false, newContext);
        }
        return context;
    };

    kafkaService.extractQuery = function (kafkaMessage) {
        var query = void 0,
            topic = void 0;

        query = JSON.parse(kafkaMessage.value).request.query;
        topic = kafkaService.makeResponseTopic(kafkaMessage);
        if (query === undefined || query === null) {
            var context = void 0;
            context = kafkaService.extractContext(kafkaMessage);
            context.response = { error: 'query is empty' };
            kafkaService.send(topic, false, context);
        } else {
            return query;
        }
    };

    kafkaService.extractWriteData = function (kafkaMessage) {
        var profile = void 0,
            topic = void 0;

        profile = JSON.parse(kafkaMessage.value).request.writeData;
        topic = kafkaService.makeResponseTopic(kafkaMessage);
        if (profile === undefined || profile === null) {
            var context = void 0;
            context = kafkaService.extractContext(kafkaMessage);
            context.response = { error: 'profile is empty' };
            kafkaService.send(topic, false, context);
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
