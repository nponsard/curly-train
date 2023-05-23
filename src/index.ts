import { Kafka, Partitioners } from "kafkajs";
import * as readline from "node:readline/promises";
import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { stdin as input, stdout as output } from "node:process";
import * as avro from "avsc";
import { Schema } from "avsc/types";
import { RawAvroSchema } from "@kafkajs/confluent-schema-registry/dist/@types";

const schema: Schema = {
  type: "record",
  name: "Message",
  namespace: "do.polytech",
  fields: [{ name: "message", type: "string" }],
};

const type = avro.Type.forSchema(schema);

const rl = readline.createInterface({ input, output });

const brokers = process.env.KAFKA_BROKERS?.split(",") || ["kafka1:9092"];
const clientId = process.env.KAFKA_CLIENT_ID || "stx";
const groupId = process.env.KAFKA_GROUP_ID || "do-group";
const topic = process.env.KAFKA_TOPIC || "do-avro";
const username = process.env.KAFKA_USERNAME || "nilsp";
const registryUrl = process.env.KAFKA_REGISTRY_URL || "http://registry:8081/";

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

let schema_id = 0;
async function send(message: string) {
  const buf = type.toBuffer({ message });

  await producer.send({
    topic,
    messages: [
      { value: buf, headers: { username, schema_id: `${schema_id}` } },
    ],
  });
}

async function run() {
  schema_id = await registry.getRegistryIdBySchema(
    subject,
    schema as RawAvroSchema
  );

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

      setTimeout(async () => {
        const random = Math.floor(Math.random() * words.length);
        const word = words[random];

        await send(word);

        console.log(`Sent ${word}`);
      }, 1000);
    },
  });

  console.log("Initialized");

  while (true) {
    const answer = await rl.question("");

    await send(answer);
  }
}

run().catch(console.error);

consumer.disconnect();
producer.disconnect();
