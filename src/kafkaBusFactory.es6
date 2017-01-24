/**
 *Created by py on 14/11/2016
 */

'use strict';
module.exports = (kafkaHost, clientName) => {
    const Kafka = require('kafka-node');
    let kafkaClient = new Kafka.Client(`${kafkaHost}:2181/`, clientName);
    let kafkaBus = {};
    kafkaBus.producer = new Kafka.Producer(kafkaClient, {partitionerType: 2});
    kafkaBus.consumer = new Kafka.Consumer(kafkaClient, []);

    return kafkaBus;
};