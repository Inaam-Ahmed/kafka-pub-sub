const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['0.0.0.0:9092']
});

const topic = 'csv-data';
const consumer = kafka.consumer({ groupId: 'test-group' });

const run = async () => {
    await consumer.connect();
    console.log('Consumer connected to Kafka');
    
    await consumer.subscribe({ topic, fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value.toString(),
            });
        },
    });
};

run().catch(console.error);
