const { getMultipleKeyDetails } = require('../../db/redis');
const { getMetadataForPids, getClient } = require('../../db/elastic');
const { initCacheWatcher, getLocalCache, updateLocalCache } = require('../cache/metadataCache');

initCacheWatcher();

async function getCoinMetadata(query) {
  let { pid } = query;
  let pidArray = [];

  if (Array.isArray(pid)) {
    pidArray = pid.map((p) => p.trim().toLowerCase()).filter(Boolean);
  } else if (typeof pid === 'string') {
    pidArray = pid.split(',').map((p) => p.trim().toLowerCase()).filter(Boolean);
  }

  if (!pidArray.length) {
    throw new Error("Parameter 'pid' is required for metadata.");
  }

  const localMetadataCache = getLocalCache();
  const resultMap = {};
  const missingPids = [];

  for (const p of pidArray) {
    if (localMetadataCache[p]) {
      resultMap[p] = localMetadataCache[p];
    } else {
      missingPids.push(p);
    }
  }

  if (missingPids.length > 0) {
    const fetchedMap = await getMetadataForPids(missingPids);
    for (const [missingPid, data] of Object.entries(fetchedMap)) {
      localMetadataCache[missingPid] = data;
      resultMap[missingPid] = data;
    }
    updateLocalCache(localMetadataCache);
  }

  for (const p of pidArray) {
    if (!resultMap[p]) {
      resultMap[p] = null;
    }
  }

  return resultMap;
}

/**
 * Retrieve the current coin data from Redis. 
 * If `withMetadata=true`, attach metadata using the same logic as getCoinMetadata.
 */
async function getCoinsService(pidString, options = {}) {
  const { withTTL = false, withMetadata = false } = options;

  const pids = pidString
    .split(',')
    .map(p => p.trim().toLowerCase())
    .filter(p => p);

  if (!pids.length) {
    throw new Error("The 'pid' query parameter must contain at least one valid pid.");
  }

  const redisKeys = pids.map(pid => `price_${pid}`);
  const coinsData = await getMultipleKeyDetails(redisKeys, withTTL);

  let metadataMap = {};
  if (withMetadata) {
    metadataMap = await getCoinMetadata({ pid: pids.join(',') });
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
      metadata: withMetadata ? (metadataMap[pid] || null) : undefined
    };
  });

  return { coins };
}

async function findClosestDocForPid(client, pid, timestampMs) {
  const afterQuery = {
    index: 'coins-timeseries-*',
    size: 1,
    body: {
      query: {
        bool: {
          must: [
            { term: { pid } },
            { range: { ts: { gte: timestampMs } } }
          ]
        }
      },
      sort: [{ ts: { order: 'asc' } }]
    }
  };

  const beforeQuery = {
    index: 'coins-timeseries-*',
    size: 1,
    body: {
      query: {
        bool: {
          must: [
            { term: { pid } },
            { range: { ts: { lte: timestampMs } } }
          ]
        }
      },
      sort: [{ ts: { order: 'desc' } }]
    }
  };

  const [afterResp, beforeResp] = await Promise.all([
    client.search(afterQuery),
    client.search(beforeQuery)
  ]);

  const afterHit = afterResp.hits?.hits[0]?._source || null;
  const beforeHit = beforeResp.hits?.hits[0]?._source || null;

  if (!afterHit && !beforeHit) return null;
  if (afterHit && !beforeHit) return afterHit;
  if (!afterHit && beforeHit) return beforeHit;

  const diffAfter = Math.abs(afterHit.ts - timestampMs);
  const diffBefore = Math.abs(beforeHit.ts - timestampMs);
  return diffAfter <= diffBefore ? afterHit : beforeHit;
}

/**
 * Retrieve timeseries data:
 * - If `timestamp` is provided, for each pid we return the doc whose ts is closest.
 * - Otherwise, we fetch all docs (or a range if startDate/endDate are given).
 */
async function getCoinsTimeseries({ pid, startDate, endDate, timestamp }) {
  const client = getClient();
  if (!client) {
    throw new Error('No Elasticsearch client available.');
  }

  const pids = pid
    .split(',')
    .map(x => x.trim().toLowerCase())
    .filter(Boolean);

  if (!pids.length) {
    throw new Error("No valid 'pid' provided.");
  }

  // CASE 1: timestamp => find the closest doc for each pid
  if (timestamp) {
    const timestampMs = parseInt(timestamp, 10) * 1000; 
    if (isNaN(timestampMs)) {
      throw new Error('Invalid timestamp format. Must be an integer in seconds.');
    }

    const results = {};
    for (const singlePid of pids) {
      const doc = await findClosestDocForPid(client, singlePid, timestampMs);
      results[singlePid] = doc;
    }
    return results;
  }

  // CASE 2: range query (startDate, endDate in ms)
  let pidQuery;
  if (pids.length === 1) {
    pidQuery = { term: { pid: pids[0] } };
  } else {
    pidQuery = { terms: { pid: pids } };
  }

  const mustQueries = [pidQuery];
  const tsRange = {};

  if (startDate) {
    tsRange.gte = new Date(startDate).getTime();
  }
  if (endDate) {
    tsRange.lte = new Date(endDate).getTime();
  }
  if (Object.keys(tsRange).length > 0) {
    mustQueries.push({ range: { ts: tsRange } });
  }

  const queryBody = {
    query: { bool: { must: mustQueries } },
    sort: [{ ts: { order: 'asc' } }],
    size: 10000
  };

  const resp = await client.search({
    index: 'coins-timeseries-*',
    body: queryBody
  });

  const hits = resp.hits?.hits || [];
  const grouped = {};

  for (const doc of hits) {
    const source = doc._source;
    const p = source.pid;
    if (!grouped[p]) {
      grouped[p] = [];
    }
    grouped[p].push(source);
  }

  for (const p of Object.keys(grouped)) {
    grouped[p].sort((a, b) => a.ts - b.ts);
  }
  return grouped;
}

module.exports = {
  getCoinMetadata,
  getCoinsService,
  getCoinsTimeseries,
};
