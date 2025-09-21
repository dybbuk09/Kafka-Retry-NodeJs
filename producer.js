const { Kafka } = require('kafkajs')

class KafkaProducer {

  producer;

  constructor() {
    const kafka = new Kafka({
      clientId: 'my-app',
      brokers: ['localhost:9092', 'localhost:9091'],
    });
    
    this.producer = kafka.producer({});
  }

  async connect() {
    await this.producer.connect()
  }

  async produce(topic, key, value, headers = {}) {
    await this.producer.send({
      topic: topic,
      messages: [
        { key: key, value: value, headers: headers },
      ],
    });
  }

}

module.exports = new KafkaProducer;