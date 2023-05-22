import { Kafka, Partitioners } from "kafkajs";
import * as readline from "node:readline/promises";

import { stdin as input, stdout as output } from "node:process";

const rl = readline.createInterface({ input, output });

const channel = new MessageChannel();
const port1 = channel.port1;

const brokers = process.env.KAFKA_BROKERS?.split(",") || ["kafka1:9092"];
const clientId = process.env.KAFKA_CLIENT_ID || "stx";
const groupId = process.env.KAFKA_GROUP_ID || "do-group";
const topic = process.env.KAFKA_TOPIC || "do";
const username = process.env.KAFKA_USERNAME || "nilsp";

const kafka = new Kafka({
  brokers,
  clientId,
});

const consumer = kafka.consumer({ groupId });
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

// french words
const words = [
  "le",
  "la",
  "de",
  "un",
  "à",
  "être",
  "et",
  "en",
  "avoir",
  "que",
  "pour",
  "dans",
  "ce",
  "il",
  "qui",
  "ne",
  "sur",
  "se",
  "pas",
  "plus",
  "pouvoir",
  "par",
  "je",
  "avec",
  "tout",
  "faire",
  "son",
  "mettre",
  "autre",
  "nous",
  "mais",
  "on",
  "ou",
  "si",
  "me",
  "manquer",
  "dire",
  "vous",
  "ici",
  "rien",
  "devoir",
  "aussi",
  "leur",
  "y",
  "prendre",
  "bien",
  "où",
  "même",
  "donner",
];

console.log(brokers);

async function run() {
  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic });

  console.log("Consumer is running");
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const text = message.value?.toString();
      const user = message.headers?.username?.toString();

      console.log(`${user}: ${text}`);

      setTimeout(async () => {
        const random = Math.floor(Math.random() * words.length);
        const word = words[random];
        await producer.send({
          topic,
          messages: [{ value: word, headers: { username } }],
        });

        console.log(`Sent ${word}`);
      }, 1000);
    },
  });

  console.log("Initialized");

  while (true) {
    const answer = await rl.question("");

    await producer.send({
      topic,
      messages: [{ value: answer, headers: { username } }],
    });
  }
}

run().catch(console.error);

consumer.disconnect();
producer.disconnect();
