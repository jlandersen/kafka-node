'use strict';

const KafkaClient = require('../../lib/kafkaClient');
const HighLevelProducer = require('../../lib/highLevelProducer');

function sendMessage (message, topic, done) {
  var client = new KafkaClient({ kafkaHost: 'localhost:9092' });
  var producer = new HighLevelProducer(client, { requireAcks: 1 });

  client.on('connect', function () {
    producer.send([{ topic: topic, messages: message, attributes: 0 }], function (error) {
      if (error) {
        done(error);
      } else {
        done(null);
      }
      producer.close(function () {});
    });
  });
}

module.exports = sendMessage;
