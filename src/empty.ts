import { Kafka } from "kafkajs";

const brokers = process.env.KAFKA_BROKERS?.split(",") || ["kafka1:9092"];
const clientId = process.env.KAFKA_CLIENT_ID || "stx";
const groupId = process.env.KAFKA_GROUP_ID || "do-group";
const topic = process.env.KAFKA_TOPIC || "do";

const kafka = new Kafka({
  brokers,
  clientId,
});

const consumer = kafka.consumer({ groupId });
const producer = kafka.producer();

async function run() {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic });

  console.log("Consumer is running");
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log("------------------------");
      console.log(message.value?.toString());
    },
  });

  console.log("Initialized");
}

run().catch(console.error);
