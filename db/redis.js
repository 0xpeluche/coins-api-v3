const Redis = require("ioredis");

let redisClient;

function getRedis() {
  if (!redisClient) {
    const redisConfig = process.env.REDIS_CLIENT_CONFIG;
    if (!redisConfig) {
      throw new Error("Missing REDIS_CLIENT_CONFIG");
    }
    const [host, port, password] = redisConfig.split("---");
    redisClient = new Redis({
      host,
      port: Number(port),
      password
    });
  }
  return redisClient;
}

async function listKeys() {
  const client = getRedis();
  try {
    const keys = await client.keys("*");
    console.log("Keys in Redis:", keys);
    return keys;
  } catch (error) {
    console.error("Error listing keys:", error);
    throw error;
  }
}

async function getKeyDetails(key, withTTL = false) {
  const client = getRedis();
  try {
    const value = await client.hgetall(key);
    let ttl = null;
    if (withTTL) {
      ttl = await client.ttl(key);
    }
    return withTTL ? { value, ttl } : { value };
  } catch (error) {
    console.error(`Error getting key ${key}:`, error);
    throw error;
  }
}

async function getMultipleKeyDetails(keys, withTTL = false) {
  const client = getRedis();
  try {
    const results = {};
    for (const key of keys) {
      const value = await client.hgetall(key);
      results[key] = { value };
      if (withTTL) {
        results[key].ttl = await client.ttl(key);
      }
    }
    return results;
  } catch (error) {
    console.error("Error getting multiple keys:", error);
    throw error;
  }
}

module.exports = { getRedis, listKeys, getKeyDetails, getMultipleKeyDetails };
