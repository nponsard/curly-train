import { Kafka } from "kafkajs";
import * as readline from "node:readline/promises";

import { stdin as input, stdout as output } from "node:process";

const rl = readline.createInterface({ input, output });

const brokers = process.env.KAFKA_BROKERS?.split(",") || ["kafka1:9092"];
const clientId = process.env.KAFKA_CLIENT_ID || "stx";
const topic = process.env.KAFKA_TOPIC || "do";

const kafka = new Kafka({
  brokers,
  clientId,
});

const producer = kafka.producer();

async function run() {
  await producer.connect();

  const text = await rl.question("> ");

  await producer.send({
    topic,
    messages: [{ value: text }],
  });
  rl.close();
}

run().catch(console.error);
