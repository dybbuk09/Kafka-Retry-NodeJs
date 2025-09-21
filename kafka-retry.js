const moment = require('moment');
const { Redis } = require('ioredis');
const { randomUUID } = require('crypto');

class KafkaRetry extends Redis {
  static keyFormat = 'YYYYMMDD-HHmm';
  static DEFAULT_DELAY = '2'; //minutes
  static DEFAULT_MAX_RETRY = '3';
  static DEFAULT_RETRY_COUNT = '0';
  static INTERVAL_TIME = 60000; //60 seconds

  super() {}

  /**
   * @description Method to push events in time based queue
   * @param topic
   * @param eventValue
   * @param options
   */
  pushToQueue = async (
    topic,
    eventValue,
    options,
  ) => {
    //Delay extra 1 minute to avoid cache miss
    const expireTime = parseInt(options.delay) + 1;
    const datetimeInstance = moment().add(expireTime, 'minutes');
    const key = datetimeInstance.format(KafkaRetry.keyFormat);
    let existingRetries = JSON.parse(await this.get(key) || '[]');
    const value = JSON.parse(eventValue.value);
    const data = {
      topic,
      event: eventValue.event,
      value: value,
      options: {
        ...options,
        retryCount: (parseInt(options.retryCount) + 1).toString(),
        executeAt: datetimeInstance.format('YYYY-MM-DD HH:mm:[00]'),
      },
    }
    existingRetries.push(data);
    await this.set(key, JSON.stringify(existingRetries))
    await this.expire(key, 60 * expireTime);
    //Set a unique message id with no expiry
    await this.set(value.msgId, "true");
  };

  /**
   * @description Lister method that will every minute
   * @param producer
   */
  queueListener = async (
    producer,
    dlq = '',
  ) => {
    setInterval(async () => {
      console.log(`###### ..... Listener triggered at ${moment().format()} ..... #######`);
      const key = moment().format(KafkaRetry.keyFormat);
      let existingRetries = JSON.parse(await this.get(key) || '[]');
      console.log(`###### ..... For Key ${key} entries exists ${existingRetries.length} ..... #######`);
      if (existingRetries?.length) {
        for (let index = 0; index < existingRetries.length; index++) {
          const ele = existingRetries[index];
          if (ele.options.retryCount <= ele.options.maxRetry) {
            //Delete message id after get so that other consumers does not consume the same message from redis
            const msgIdExists = await this.getdel(ele.value.msgId);
            if (msgIdExists) {
              //Update the message id for next message
              ele.value.msgId = randomUUID();
              await producer.produce(
                ele.topic,
                ele.event,
                JSON.stringify(ele.value),
                ele.options,
              );
            }
          } else {
            //On reaching the max retry limit push the ele to dead letter queue
            if (dlq) {
              console.log(`Pushing the event to ${dlq} with event ${ele.event}`,);
              await this.pushToDlq(dlq, ele);
            }
          }
        }
      }
    }, KafkaRetry.INTERVAL_TIME);
  };

  static headers = (headers) => {
    return {
      delay: headers.delay
        ? Buffer.from(headers.delay, 'utf-8').toString()
        : KafkaRetry.DEFAULT_DELAY,
      maxRetry: headers.maxRetry
        ? Buffer.from(headers.maxRetry, 'utf-8').toString()
        : KafkaRetry.DEFAULT_MAX_RETRY,
      retryCount: headers.retryCount
        ? Buffer.from(headers.retryCount, 'utf-8').toString()
        : KafkaRetry.DEFAULT_RETRY_COUNT,
    };
  };

  /**
   * @description Push the failed events to a dead letter queue for further processing
   * @param ele
   */
  pushToDlq = async (dlq, ele) => {
    //Redis hash is being used for dead letter queue
    await this.hset(
      dlq,
      { 
        [ele.event]: JSON.stringify({
          value: ele.value,
          headers: ele.headers,
        }) 
      }
    )
  };
}

module.exports = {
  KafkaRetry
};
