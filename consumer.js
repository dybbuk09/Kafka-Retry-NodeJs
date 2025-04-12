
const { Kafka } = require('kafkajs');
const { KafkaRetry } = require('./kafka-retry');

class KafkaConsumer {

    consumer;

    constructor() {
        const kafka = new Kafka({
            clientId: 'kafka-retry',
            brokers: ['localhost:9092', 'localhost:9091'],
        })

        this.consumer = kafka.consumer({ groupId: 'test-group' })
    }

    async connect() {
        await this.consumer.connect()
    }

    async consumerFromTopic(topic) {
        await this.consumer.subscribe({ topic: topic, fromBeginning: true })
        await this.consumer.run({
            eachMessage: async ({ topic, partition, message }) => {
                const key = message.key.toString();
                const value = JSON.parse(message.value.toString());
                const headers = message.headers;
                
                if (key == 'KafkaRetry') {
                    if (!value.success ) {
                        try {
                            throw 'Action failed';
                        } catch (error) {
                            new KafkaRetry().pushToQueue(
                                topic,
                                {
                                    event: key,
                                    value: JSON.stringify(value)
                                },
                                KafkaRetry.headers(headers)
                            )
                        }   
                    }
                }
            },
        })
    }
}

module.exports = new KafkaConsumer;KafkaRetry