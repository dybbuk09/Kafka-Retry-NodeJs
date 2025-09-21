# Kafka Retry Mechanism

This is a simple implementation demonstrating how to build a retry queue with backoff for Kafka using Redis in NodeJs.

## Overview

When working with message brokers like Kafka, it's common to encounter scenarios where message processing can fail due to transient issues. Instead of discarding these messages or blocking the main processing stream, a retry mechanism allows you to reprocess them after a delay.

This implementation uses Redis to manage retry attempts and delays, helping ensure reliable message processing and eventual consistency.

## Features

- Redis-backed retry queue
- Configurable delay and retry attempts

## Requirements

- Kafka
- Redis
- NodeJs
- Kafka client library
- Redis client library

## Usage

1. After running producer and consumer. Run KafkaRetry "queueListener"
```
await new KafkaRetry().queueListener(new Producer(), "DEAD_LETTER_QUEUE_NAME");

```

2. Produce the message. Example:
```
async produce(topic, key, value, headers = {}) {
    await this.producer.send({
        topic: topic,
        messages: [
           { key: key, value: value, headers: headers },
        ],
    })
 }
```

3. In consumer if any event fails push that to queue. Example:
```
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
```

4. Implementation server.js
```
const KafkaProducer = require('./producer');
const KafkaConsumer = require('./consumer');
const { KafkaRetry } = require('./kafka-retry');
const { TOPIC, DEAD_LETTER_QUEUE, EVENT_NAME } = require('./constants');
const { randomUUID } = require('crypto');

const init = async () => {
    await KafkaProducer.connect();
    await KafkaConsumer.connect();
    await KafkaConsumer.consumerFromTopic(TOPIC);
    await new KafkaRetry().queueListener(KafkaProducer, DEAD_LETTER_QUEUE);
    //Add msgId to handle deduplication of messages while picking from the redis queue
    const examplePayload = JSON.stringify({ msgId: randomUUID(), success: false });
    await KafkaProducer.produce(TOPIC, EVENT_NAME, examplePayload);
}

init();
```