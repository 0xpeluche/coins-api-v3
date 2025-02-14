
const { Kafka } = require("kafkajs");

let kafka, producer;
const consumers = {};

function getKafka() {
  if (!kafka) {
    const kafkaConfig = process.env.KAFKA_CLIENT_CONFIG;
    if (!kafkaConfig) {
      throw new Error("Missing KAFKA_CLIENT_CONFIG");
    }
    const [brokers, username, password] = kafkaConfig.split("---");
    kafka = new Kafka({
      clientId: "my-app",
      brokers: brokers.split(","),
      ssl: {
        rejectUnauthorized: false, // Allow self-signed certificates
      },
      sasl: { mechanism: "scram-sha-256", username, password },
    });
  }
  return kafka;
}


async function getConsumer(groupId) {
  if (!groupId) throw new Error("Missing groupId");
  if (!consumers[groupId])
    consumers[groupId] = getKafka().consumer({ groupId });
  await consumers[groupId].connect();
  return consumers[groupId];
}


async function getProducer() {
  if (!producer) producer = getKafka().producer();
  await producer.connect();
  return producer;
}

module.exports = { getConsumer, getProducer };