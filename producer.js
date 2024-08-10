const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'garbage-collection-system',
    brokers: ['localhost:9092'],
});

const producer = kafka.producer();

const startProducer = async () => {
    await producer.connect();
    console.log('Producer connected');
};

const sendMessage = async (location) => {
    await producer.send({
        topic: 'location',
        messages: [
            { value: JSON.stringify(location) },
        ],
    });
};

module.exports = { startProducer, sendMessage };