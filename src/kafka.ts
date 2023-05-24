import { SchemaRegistry } from "@kafkajs/confluent-schema-registry";
import { schema } from "./schema";
import * as avro from "avsc";
import { Kafka, Partitioners } from "kafkajs";
import { sendRandom } from "./random";

const brokers = process.env.KAFKA_BROKERS?.split(",") || [
  "162.38.112.138:9092",
];
const clientId = process.env.KAFKA_CLIENT_ID || "stx";
const groupId = process.env.KAFKA_GROUP_ID || "do-group";
const topic = process.env.KAFKA_TOPIC || "do-avro";
const username = process.env.KAFKA_USERNAME || "nilsp";
const registryUrl =
  process.env.KAFKA_REGISTRY_URL || "http://162.38.112.138:8081/";

const subject = "do-avro-std-value";

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

const type = avro.Type.forSchema(schema);
let schema_id = 0;
export async function send(message: string) {
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

export async function init() {
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
}

export async function close() {
  await consumer.disconnect().catch(console.error);
  await producer.disconnect().catch(console.error);
}
