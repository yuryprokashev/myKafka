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
            let error = new Error(`producer error\n${err}`);
            kafkaService.emit('error', error);
        };

        onProducerSent = (err, data) => {
            if(err){
                let error = new Error(`producer sent error\n${err}`);
                kafkaService.emit('error', error);

            }
            if(data){
                kafkaService.emit('log', `producer sent success\n${data}`);
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
                let error = new Error(`consumer failed to add topics\n${err}`);
                kafkaService.emit('error', error);
            }
        };

        onConsumerMessage = message => {
            if(message.topic === topic) {
                callback(message);
            }
        };

        onConsumerError = (err) => {
            let error = new Error(`consumer default error\n${err}`);
            kafkaService.emit('error', error);
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
            let error = new Error('unable to extract id, kafkaMessage has no context');
            kafkaService.emit('error', error);
            return error;
        }

    };

    kafkaService.createContext = (signature, query, data) => {
        if(signature === undefined || typeof signature !== 'string'){
            let error = new Error(`wrong signature passed:\n${signature}`);
            kafkaService.emit('error', error);
            return error;
        }
        if(query === undefined || typeof query !== "object") {
            let error = new Error(`wrong query passed:\n${query}`);
            kafkaService.emit('error', error);
            return error;
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
            let error = new Error('kafkaMessage has no value');
            kafkaService.emit('error', error);
            return error;
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
            let error = new Error('unable to extract response, kafkaMessage has no context');
            kafkaService.emit('error', error);
            return error;
        }
    };

    kafkaService.extractQuery = kafkaMessage => {
        let query;

        query = JSON.parse(kafkaMessage.value).request.query;

        if(query === undefined || query === null) {
            let error = new Error('kafkaMessage has no query');
            kafkaService.emit('error', error);
            return error;
        }
        else {
            return query;
        }
    };

    kafkaService.extractWriteData = kafkaMessage => {
        let writeData;

        writeData = JSON.parse(kafkaMessage.value).request.writeData;

        if(writeData === undefined || writeData === null) {

            let error = new Error('kafkaMessage has no writeData');
            kafkaService.emit('error', error);
            return error;
        }
        else {
            return writeData;
        }
    };

    kafkaService.makeResponseTopic = kafkaMessage => {
        let re = /-request/;
        return kafkaMessage.topic.replace(re, '-response');
    };

    kafkaService.isMyMessage = (mySignature, kafkaMessage) => {
        return mySignature === kafkaService.extractId(kafkaMessage);
    };

    return kafkaService;
};
