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
            var logMessage = kafkaService.packLogMessage(undefined, 'producer error\n' + err);
            kafkaService.emit('logger.agent.error', logMessage);
        };

        onProducerSent = function onProducerSent(err, data) {
            if (err) {
                var logMessage = kafkaService.packLogMessage(undefined, 'producer sent error\n' + err);
                kafkaService.emit('logger.agent.error', logMessage);
            }
            if (data) {
                var _logMessage = kafkaService.packLogMessage(undefined, 'producer sent success\n' + data);
                kafkaService.emit('logger.agent.log', _logMessage);
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
                var logMessage = kafkaService.packLogMessage(undefined, 'consumer failed to add topics\n' + err);
                kafkaService.emit('logger.agent.error', logMessage);
            }
        };

        onConsumerMessage = function onConsumerMessage(message) {
            if (message.topic === topic) {
                callback(message);
            }
        };

        onConsumerError = function onConsumerError(err) {
            var logMessage = kafkaService.packLogMessage(undefined, 'consumer default error\n' + err);
            kafkaService.emit('logger.agent.error', logMessage);
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
            return kafkaService.packLogMessage(undefined, 'unable to extract id, kafkaMessage has no context');
        }
    };

    kafkaService.createContext = function (signature, query, data) {
        if (signature === undefined || typeof signature !== 'string') {
            return kafkaService.packLogMessage(undefined, 'wrong signature passed:\n' + signature);
        }
        if (query === undefined || (typeof query === 'undefined' ? 'undefined' : _typeof(query)) !== "object") {
            return kafkaService.packLogMessage(undefined, 'wrong query passed:\n' + query);
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
            return kafkaService.packLogMessage(undefined, 'kafkaMessage has no value');
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
            return kafkaService.packLogMessage(undefined, 'unable to extract response, kafkaMessage has no context');
        }
    };

    kafkaService.extractQuery = function (kafkaMessage) {
        var query = void 0;

        query = JSON.parse(kafkaMessage.value).request.query;

        if (query === undefined || query === null) {
            return kafkaService.packLogMessage(undefined, 'kafkaMessage has no query');
        } else {
            return query;
        }
    };

    kafkaService.extractWriteData = function (kafkaMessage) {
        var writeData = void 0;

        writeData = JSON.parse(kafkaMessage.value).request.writeData;

        if (writeData === undefined || writeData === null) {
            return kafkaService.packLogMessage(undefined, 'kafkaMessage has no writeData');
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
