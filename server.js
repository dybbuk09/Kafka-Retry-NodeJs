const KafkaProducer = require('./producer');
const KafkaConsumer = require('./consumer');
const { KafkaRetry } = require('./kafka-retry');
const { TOPIC, DEAD_LETTER_QUEUE, EVENT_NAME } = require('./constants');

const init = async () => {
    await KafkaProducer.connect()
    await KafkaConsumer.connect();
    await KafkaConsumer.consumerFromTopic(TOPIC)
    await new KafkaRetry().queueListener(KafkaProducer, DEAD_LETTER_QUEUE);
    const examplePayload = JSON.stringify({ success: false });
    await KafkaProducer.produce(TOPIC, EVENT_NAME, examplePayload);
}

init();