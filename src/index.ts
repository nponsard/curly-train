import { Kafka, Partitioners } from "kafkajs";
import * as readline from "node:readline/promises";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { stdin as input, stdout as output } from "node:process";
import * as avro from "avsc";
import { schema } from "./schema";
import { words } from "./words";

const type = avro.Type.forSchema(schema);

const rl = readline.createInterface({ input, output });

const brokers = process.env.KAFKA_BROKERS?.split(",") || [
  "162.38.112.138:9092",
];
const clientId = process.env.KAFKA_CLIENT_ID || "stx";
const groupId = process.env.KAFKA_GROUP_ID || "do-group";
const topic = process.env.KAFKA_TOPIC || "do-avro";
const username = process.env.KAFKA_USERNAME || "nilsp";
const registryUrl =
  process.env.KAFKA_REGISTRY_URL || "http://162.38.112.138:8081/";

const subject = "do.polytech.Message";

const registry = new SchemaRegistry({ host: registryUrl });

const kafka = new Kafka({
  brokers,
  clientId,
});

const consumer = kafka.consumer({ groupId });
const producer = kafka.producer({
  createPartitioner: Partitioners.DefaultPartitioner,
});

console.log(brokers);

let schema_id = 0;
async function send(message: string) {
  const buf = type.toBuffer({ message });

  console.log("sending", message, "with schema id", schema_id);

  console.log(buf);

  await producer.send({
    topic,
    messages: [
      { value: buf, headers: { username, schema_id: `${schema_id}` } },
    ],
  });
}

let timeout: NodeJS.Timeout | null = null;

function sendRandom() {
  // if (timeout) return;
  timeout = setTimeout(async () => {
    const random = Math.floor(Math.random() * words.length);
    const word = words[random];

    await send(word);

    console.log(`Sent ${word}`);
    timeout = null;
  }, 1000);
}

async function run() {
  schema_id = await registry.getRegistryIdBySchema(subject, schema);

  await consumer.connect();
  await producer.connect();

  await consumer.subscribe({ topic });

  console.log("Consumer is running");
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      if (message.value === null) return;

      const text = type.fromBuffer(message.value)?.message?.toString();
      const user = message.headers?.username?.toString();
      const recieved_schema_id = message.headers?.schema_id?.toString();

      console.log(`${user} (${recieved_schema_id}): ${text}`);
      sendRandom();
    },
  });

  console.log("Initialized");

  while (true) {
    const answer = await rl.question("");

    await send(answer);
  }
}

run().catch(console.error);

consumer.disconnect().catch(console.error);
producer.disconnect().catch(console.error);
