/**
 *Created by py on 07/02/2017
 */

"use strict";

module.exports = (kafkaBus, EventEmitter) =>{

    let kafkaService = new EventEmitter();

    kafkaService.send = (topic, message) => {

        let onProducerError,
            onProducerSent;

        onProducerError = (err) => {
            let logMessage = kafkaService.packLogMessage(this, `producer error\n${err}`);
            kafkaService.emit('logger.agent.error', logMessage);
        };

        onProducerSent = (err, data) => {
            if(err){
                let logMessage = kafkaService.packLogMessage(this, `producer sent error\n${err}`);
                kafkaService.emit('logger.agent.error', logMessage);

            }
            if(data){
                let logMessage = kafkaService.packLogMessage(this, `producer sent success\n${data}`);
                kafkaService.emit('logger.agent.log', logMessage);
            }
        };

        kafkaBus.producer.on('error', onProducerError);
        kafkaBus.producer.send([{topic: topic, messages: JSON.stringify(message)}], onProducerSent);
    };

    kafkaService.subscribe = (topic, callback) => {

        let topics;
        let onTopicsAdded,
            onConsumerMessage,
            onConsumerError;

        onTopicsAdded = (err, added) => {
            if(err){
                let logMessage = kafkaService.packLogMessage(this, `consumer failed to add topics\n${err}`);
                kafkaService.emit('logger.agent.error', logMessage);
            }
        };

        onConsumerMessage = message => {
            if(message.topic === topic) {
                callback(message);
            }
        };

        onConsumerError = (err) => {
            let logMessage = kafkaService.packLogMessage(this, `consumer default error\n${err}`);
            kafkaService.emit('logger.agent.error', logMessage);
        };

        topics = (function(qty){
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
            return kafkaService.packLogMessage(this, 'unable to extract id, kafkaMessage has no context');
        }

    };

    kafkaService.createContext = (signature, query, data) => {
        if(signature === undefined || typeof signature !== 'string'){
            return kafkaService.packLogMessage(this, `wrong signature passed:\n${signature}`);
        }
        if(query === undefined || typeof query !== "object") {
            return kafkaService.packLogMessage(this, `wrong query passed:\n${query}`);
        }
        else {
            return {
                id: signature,
                request: {
                    query: query,
                    writeData: data || undefined
                },
                response: undefined
            }
        }
    };

    kafkaService.extractContext = kafkaMessage => {
        let context;

        context = JSON.parse(kafkaMessage.value);

        if(context === undefined || context === null) {
            return kafkaService.packLogMessage(this, 'kafkaMessage has no value');
        }
        else {
            return context;
        }
    };

    kafkaService.extractResponse = kafkaMessage => {
        let context, response;

        context = kafkaService.extractContext(kafkaMessage);
        if(context.error === undefined) {
            response = context.response;
            return response;
        }
        else {
            return kafkaService.packLogMessage(this, 'unable to extract response, kafkaMessage has no context');
        }
    };

    kafkaService.extractQuery = kafkaMessage => {
        let query;

        query = JSON.parse(kafkaMessage.value).request.query;

        if(query === undefined || query === null) {
            return kafkaService.packLogMessage(this, 'kafkaMessage has no query');
        }
        else {
            return query;
        }
    };

    kafkaService.extractWriteData = kafkaMessage => {
        let writeData;

        writeData = JSON.parse(kafkaMessage.value).request.writeData;

        if(writeData === undefined || writeData === null) {
            return kafkaService.packLogMessage(this, 'kafkaMessage has no writeData');
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
