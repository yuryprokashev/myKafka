/**
 *Created by py on 14/11/2016
 */

'use strict';

module.exports = function (kafkaHost, clientName) {
    var Kafka = require('kafka-node');
    var kafkaClient = new Kafka.Client(kafkaHost + ':2181/', clientName);
    var kafkaBus = {};
    kafkaBus.producer = new Kafka.Producer(kafkaClient, { partitionerType: 2 });
    kafkaBus.consumer = new Kafka.Consumer(kafkaClient, []);

    return kafkaBus;
};
