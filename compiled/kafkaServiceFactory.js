/**
 *Created by py on 14/11/2016
 */
"use strict";

module.exports = function (kafkaBus) {

    var kafkaService = {};

    kafkaService.send = function (topic, message) {
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
                console.log('-------------------');
            }
        };
        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{ topic: topic, messages: JSON.stringify(message) }], onProducerSent);
    };

    kafkaService.subscribe = function (topic, callback) {
        var onTopicsAdded = function onTopicsAdded(err, added) {
            if (err) {
                console.log('consumer failed to add topics');
                console.log(err);
                console.log('-------------');
            }
        };
        var onConsumerMessage = function onConsumerMessage(message) {
            if (message.topic === topic) {
                callback(message);
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

    kafkaService.extractContext = function (kafkaMessage) {
        var context = void 0;
        context = JSON.parse(kafkaMessage.value);
        if (context === undefined) {
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
