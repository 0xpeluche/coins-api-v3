const { getMultipleKeyDetails } = require("../../db/redis");
const { getMetadataForPids } = require("../../db/elastic");

async function getCoinsService(pids, options = {}) {
  const { withTTL = false, withMetadata = false } = options;

  const redisKeys = pids.map(pid => `price_${pid}`);
  const coinsData = await getMultipleKeyDetails(redisKeys, withTTL);

  let metadataMap = {};
  if (withMetadata) {
    metadataMap = await getMetadataForPids(pids);
  }

  const coins = pids.map(pid => {
    const redisKey = `price_${pid}`;
    const data = coinsData[redisKey] || { value: {}, ttl: null };
    const { pid: storedPid, price, confidence, source } = data.value;

    return {
      pid: storedPid || pid,
      price,
      confidence,
      source,
      ttl: withTTL ? data.ttl : undefined,
      metadata: withMetadata ? metadataMap[pid] || null : undefined
    };
  });

  return { coins };
}

module.exports = { getCoinsService };

