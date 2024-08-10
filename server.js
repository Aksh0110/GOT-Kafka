const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { startProducer, sendMessage } = require('./producer');
const { startConsumer } = require('./consumer');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);
app.use(express.json())
app.use(express.static('public'));
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'garbage-collection-system',
    brokers: ['localhost:9092'],
});
const consumer = kafka.consumer({ groupId: 'location-group' });
io.on('connection', async (socket) => {
    console.log('Client connected');
    // io.emit("get-location", await startConsumer())
    socket.on("save-location", (data) => {
        const { latitude, longitude } = data;
        console.log("Save location", data)
        sendMessage({
            latitude, longitude
        })
    })

    let latestLocation = null;

    // Run Kafka consumer to update the latest location data
    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            latestLocation = JSON.parse(message.value.toString());
            console.log(`Received location: ${latestLocation.latitude}, ${latestLocation.longitude}`);
        },
    }).catch(err => {
        console.error('Error in Kafka consumer:', err);
    });

    // Emit the latest location data to the client every 2 seconds
    const interval = setInterval(() => {
        if (latestLocation) {
            socket.emit('get-location', latestLocation);
            console.log('Sent location:', latestLocation);
        } else {
            console.log('No location data received yet');
        }
    }, 2000);
    // socket.on('get-location', async () => {
        //     console.log('Start sharing location');
        //     // Simulate location updates
        //     await startConsumer();

        //     socket.on('stop', () => {
        //         console.log('Stop sharing location');
        //         clearInterval(interval);
        //     });
        // });

        socket.on('disconnect', () => {
            console.log('Client disconnected');
        });
    });

    app.post("/save-location", (req, res) => {
        try {
            const { latitude, longitude } = req.body;

            console.log("Latitude", latitude, longitude)
            sendMessage({
                latitude, longitude
            });
        } catch (error) {
            console.log("Save Location", error)
        }
    })
    app.get("/get-location", async (req, res) => {
        // Get 5 meter data logic
        io.emit("get-location", async () => {

            await consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    const location = JSON.parse(message.value.toString());
                    console.log(`Received location: ${location.latitude}, ${location.longitude}`);
                },
            });
        })

    })

    const start = async () => {
        await startProducer();
        // await startConsumer();
        await consumer.connect();
        console.log('Consumer connected');
        await consumer.subscribe({ topic: 'location', fromBeginning: true });
        server.listen(3000, () => {
            console.log('Server is running on http://localhost:3000');
        });
    };

    start();