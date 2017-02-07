/**
 *Created by py on 14/11/2016
 */
"use strict";

var _arguments = arguments;
var guid = require('./guid');
module.exports = function (kafkaBus) {

    var kafkaService = {};
    var signedRequests = new Map();

    kafkaService.send = function (topic, signature, message) {
        var args = [].concat(Array.prototype.slice.call(_arguments));

        if (args.length === 2) {
            message = signature;
            signature = undefined;
        }

        if (signature !== undefined) {
            message.id = signature;
            signedRequests.set(signature, message);
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
                console.log('message.response sent');
                console.log(message.response);
                console.log('-------------------');
            }
        };
        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{ topic: topic, messages: JSON.stringify(message) }], onProducerSent);
    };

    kafkaService.subscribe = function (topic, signature, callback) {

        var args = [].concat(Array.prototype.slice.call(_arguments));
        console.log(_arguments);
        console.log(args);

        if (args.length === 2) {
            callback = signature;
            signature = undefined;
        }

        var onTopicsAdded = function onTopicsAdded(err, added) {
            if (err) {
                console.log('consumer failed to add topics');
                console.log(err);
                console.log('-------------');
            }
        };

        var onConsumerMessage = function onConsumerMessage(message) {
            console.log('signature = ' + signature);
            if (signature === undefined) {
                if (message.topic === topic) {
                    callback(message);
                }
            } else if (signature !== undefined) {
                var messageSignature = void 0;
                messageSignature = kafkaService.extractId(message);
                if (messageSignature.error !== undefined) {
                    console.log(messageSignature.error);
                }

                if (signedRequests.has(signature)) {
                    callback(message);
                } else {
                    console.log('message arrived, it has unknown signature, no callback executed');
                }
            } else {
                console.log('message has signature and has no signature');
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
        if (context.error === undefined) {
            id = context.id;
            return id;
        } else {
            return { error: 'unable to extract id, kafkaMessage has no context' };
        }
    };

    kafkaService.extractContext = function (kafkaMessage) {
        var context = void 0;

        context = JSON.parse(kafkaMessage.value);

        if (context === undefined || context === null) {
            return { error: 'kafkaMessage has no value' };
        }
        return context;
    };

    kafkaService.extractQuery = function (kafkaMessage) {
        var query = void 0;

        query = JSON.parse(kafkaMessage.value).request.query;
        if (query === undefined || query === null) {
            return { error: 'kafkaMessage has no query' };
        } else {
            return query;
        }
    };

    kafkaService.extractWriteData = function (kafkaMessage) {
        var writeData = void 0;

        writeData = JSON.parse(kafkaMessage.value).request.writeData;
        if (writeData === undefined || writeData === null) {
            return { error: 'kafkaMessage has no writeData' };
        } else {
            return writeData;
        }
    };

    kafkaService.makeResponseTopic = function (kafkaMessage) {
        var re = /-request/;
        return kafkaMessage.topic.replace(re, '-response');
    };

    return kafkaService;
};
