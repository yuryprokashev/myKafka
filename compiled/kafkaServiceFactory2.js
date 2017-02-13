/**
 *Created by py on 07/02/2017
 */

"use strict";

var _typeof = typeof Symbol === "function" && typeof Symbol.iterator === "symbol" ? function (obj) { return typeof obj; } : function (obj) { return obj && typeof Symbol === "function" && obj.constructor === Symbol ? "symbol" : typeof obj; };

module.exports = function (kafkaBus, EventEmitter) {

    var kafkaService = new EventEmitter();

    kafkaService.send = function (topic, message) {

        var onProducerError = void 0,
            onProducerSent = void 0;

        onProducerError = function onProducerError(err) {
            var error = new Error('producer error\n' + JSON.stringify(err));
            kafkaService.emit('error', error);
        };

        onProducerSent = function onProducerSent(err, data) {
            if (err) {
                var error = new Error('producer sent error\n' + JSON.stringify(err));
                kafkaService.emit('error', error);
            }
            if (data) {
                kafkaService.emit('log', 'producer sent success\n' + JSON.stringify(data));
            }
        };

        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{ topic: topic, messages: JSON.stringify(message) }], onProducerSent);
    };

    kafkaService.subscribe = function (topic, callback) {

        var topics = void 0;
        var onTopicsAdded = void 0,
            onConsumerMessage = void 0,
            onConsumerError = void 0;

        onTopicsAdded = function onTopicsAdded(err, added) {
            if (err) {
                var error = new Error('consumer failed to add topics\n' + err);
                kafkaService.emit('error', error);
            }
        };

        onConsumerMessage = function onConsumerMessage(message) {
            if (message.topic === topic) {
                callback(message);
            }
        };

        onConsumerError = function onConsumerError(err) {
            var error = new Error('consumer default error\n' + err);
            kafkaService.emit('error', error);
        };

        topics = function (qty) {
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
            var error = new Error('unable to extract id, kafkaMessage has no context');
            // kafkaService.emit('error', error);
            return error;
        }
    };

    kafkaService.createContext = function (signature, query, data) {
        if (signature === undefined || typeof signature !== 'string') {
            var error = new Error('wrong signature passed:\n' + signature);
            // kafkaService.emit('error', error);
            return error;
        }
        if (query === undefined || (typeof query === 'undefined' ? 'undefined' : _typeof(query)) !== "object") {
            var _error = new Error('wrong query passed:\n' + query);
            // kafkaService.emit('error', error);
            return _error;
        } else {
            return {
                id: signature,
                request: {
                    query: query,
                    writeData: data || undefined
                },
                response: undefined
            };
        }
    };

    kafkaService.extractContext = function (kafkaMessage) {
        var context = void 0;

        context = JSON.parse(kafkaMessage.value);

        if (context === undefined || context === null) {
            var error = new Error('kafkaMessage has no value');
            // kafkaService.emit('error', error);
            return error;
        } else {
            return context;
        }
    };

    kafkaService.extractResponse = function (kafkaMessage) {
        var context = void 0,
            response = void 0;

        context = kafkaService.extractContext(kafkaMessage);
        if (context.error === undefined) {
            response = context.response;
            return response;
        } else {
            var error = new Error('unable to extract response, kafkaMessage has no context');
            // kafkaService.emit('error', error);
            return error;
        }
    };

    kafkaService.extractQuery = function (kafkaMessage) {
        var query = void 0;

        query = JSON.parse(kafkaMessage.value).request.query;

        if (query === undefined || query === null) {
            var error = new Error('kafkaMessage has no query');
            // kafkaService.emit('error', error);
            return error;
        } else {
            return query;
        }
    };

    kafkaService.extractWriteData = function (kafkaMessage) {
        var writeData = void 0;

        writeData = JSON.parse(kafkaMessage.value).request.writeData;

        if (writeData === undefined || writeData === null) {

            var error = new Error('kafkaMessage has no writeData');
            // kafkaService.emit('error', error);
            return error;
        } else {
            return writeData;
        }
    };

    kafkaService.makeResponseTopic = function (kafkaMessage) {
        var re = /-request/;
        return kafkaMessage.topic.replace(re, '-response');
    };

    kafkaService.isMyMessage = function (mySignature, kafkaMessage) {
        var messageSignature = kafkaService.extractId(kafkaMessage);
        if (messageSignature instanceof Error) {
            // kafkaService.emit('error', messageSignature);
            return messageSignature;
        } else {
            return mySignature === messageSignature;
        }
    };

    return kafkaService;
};
