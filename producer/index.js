const { Kafka } = require('kafkajs');
const fs = require('fs');
const csvParser = require('csv-parser');



const kafka = new Kafka({
    clientId: 'my-app',
    brokers: ['0.0.0.0:9092']
});

const topic = 'csv-data';
const producer = kafka.producer();

const run = async () => {
    await producer.connect();
    console.log('Producer connected to Kafka');

    fs.createReadStream('./data.csv')
        .pipe(csvParser())
        .on('data', async (row) => {
            await producer.send({
                topic,
                messages: [
                    { value: JSON.stringify(row) },
                ],
            });
            console.log('Data sent:', row);
        })
        .on('end', () => {
            console.log('CSV file successfully processed');
        });
};

run().catch(console.error);
