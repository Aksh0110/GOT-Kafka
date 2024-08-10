const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'garbage-collection-system',
    brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'location-group' });

const startConsumer = async () => {
    await consumer.connect();
    console.log('Consumer connected');
    await consumer.subscribe({ topic: 'location', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            const location = JSON.parse(message.value.toString());
            console.log(`Received location: ${location.latitude}, ${location.longitude}`);
        },
    });
};

module.exports = { startConsumer };