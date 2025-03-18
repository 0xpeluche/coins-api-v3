const { getMultipleKeyDetails } = require('../../db/redis');
const { getCoinMetadata } = require('./getMetadataCoin')
const { getEffectivePids, parsePidMapping } = require('../../utils/index')

/**
 * Retrieve the current coin data from Redis.
 * Always attach metadata using getCoinMetadata.
 * Returns an object mapping the original PID to its coin data.
 */
async function getCurrentCoin(pidString, options = {}) {
  const { withTTL = false } = options;
  const { mapping, normalizedPids } = parsePidMapping(pidString);
  if (!normalizedPids.length) {
    throw new Error("The 'pid' query parameter must contain at least one valid pid.");
  }

  const metadataMap = getCoinMetadata({ pid: pidString });
  for (const originalPid in mapping) {
    if (!metadataMap[originalPid]) {
      metadataMap[originalPid] = {};
    }
  }

  const unionCandidatesSet = new Set();
  for (const originalPid in mapping) {
    const effectiveCandidates = getEffectivePids(originalPid, mapping, metadataMap);
    effectiveCandidates.forEach(candidate => unionCandidatesSet.add(candidate));
  }

  const unionCandidates = Array.from(unionCandidatesSet);
  const redisKeys = unionCandidates.map(pid => `price_${pid}`);
  const coinsData = await getMultipleKeyDetails(redisKeys, withTTL);
  
  const coins = {};
  for (const originalPid in mapping) {
    const candidates = getEffectivePids(originalPid, mapping, metadataMap);
    let data = null;
    for (const candidate of candidates) {
      const redisKey = `price_${candidate}`;
      if (coinsData[redisKey] && coinsData[redisKey].value && Object.keys(coinsData[redisKey].value).length > 0) {
        data = coinsData[redisKey];
        break;
      }
    }
    if (!data) {
      data = { value: {}, ttl: null };
    }
    const { price, confidence, source } = data.value;
    const { address, symbol, decimals, chain } = metadataMap[originalPid]
    coins[originalPid] = {
      pid: originalPid,
      address,
      symbol,
      chain,
      decimals,
      price,
      confidence,
      source,
      ttl: withTTL ? data.ttl : undefined,
    };
  }
  
  return { coins };
}

module.exports = { getCurrentCoin }