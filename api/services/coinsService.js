const fs = require('fs');
const path = require('path');
const { getMultipleKeyDetails } = require('../../db/redis');
const { getMetadataForPids, getClient } = require('../../db/elastic');

let localMetadataCache = {};

const METADATA_JSON_PATH = path.join(__dirname, '../data/metadata.json');

function loadLocalMetadataCache() {
  try {
    const data = fs.readFileSync(METADATA_JSON_PATH, 'utf8');
    localMetadataCache = JSON.parse(data);
  } catch (err) {
    localMetadataCache = {};
  }
}

function saveLocalMetadataCache() {
  try {
    fs.writeFileSync(
      METADATA_JSON_PATH,
      JSON.stringify(localMetadataCache, null, 2),
      'utf8'
    );
    console.log(`Cache saved to ${METADATA_JSON_PATH}`);
  } catch (err) {
    console.error('Failed to write local metadata cache file:', err);
  }
}

loadLocalMetadataCache();

async function getCoinMetadata(query) {
  let { pid } = query;
  let pidArray = [];

  if (Array.isArray(pid)) {
    pidArray = pid.map((p) => p.trim().toLowerCase()).filter((p) => p);
  } else if (typeof pid === 'string') {
    pidArray = pid.split(',').map((p) => p.trim().toLowerCase()).filter((p) => p);
  }

  if (!pidArray.length) {
    throw new Error("Parameter 'pid' is required for metadata.");
  }

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
    saveLocalMetadataCache();
  }

  for (const p of pidArray) {
    if (!resultMap[p]) {
      resultMap[p] = null;
    }
  }

  return resultMap;
}

async function getCoinsService(pidString, options = {}) {
  const { withTTL = false, withMetadata = false } = options;

  const pids = pidString
    .split(',')
    .map((p) => p.trim().toLowerCase())
    .filter((p) => p);

  if (!pids.length) {
    throw new Error("The 'pid' query parameter must contain at least one valid pid.");
  }

  const redisKeys = pids.map((pid) => `price_${pid}`);
  const coinsData = await getMultipleKeyDetails(redisKeys, withTTL);

  let metadataMap = {};
  if (withMetadata) {
    metadataMap = await getCoinMetadata({ pid: pids.join(',') });
  }

  const coins = pids.map((pid) => {
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

/**
 * Internal helper to find the single document closest to a given timestamp (in seconds) for a specific pid.
 * This approach uses two queries:
 *  1) The doc with ts >= timestamp, sorted asc, size=1
 *  2) The doc with ts <= timestamp, sorted desc, size=1
 * We pick whichever is closer, or return null if neither exist.
 */
async function findClosestDocForPid(client, pid, timestampNum) {
  const afterQuery = {
    index: 'coins-timeseries-*',
    size: 1,
    body: {
      query: {
        bool: {
          must: [
            { term: { pid } },
            { range: { ts: { gte: timestampNum } } }
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
            { range: { ts: { lte: timestampNum } } }
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

  if (!afterHit && !beforeHit) {
    return null;
  }
  if (afterHit && !beforeHit) {
    return afterHit;
  }
  if (!afterHit && beforeHit) {
    return beforeHit;
  }

  const diffAfter = Math.abs(afterHit.ts - timestampNum);
  const diffBefore = Math.abs(beforeHit.ts - timestampNum);

  return diffAfter < diffBefore ? afterHit : beforeHit;
}

/**
 * Retrieve timeseries data.
 *  - pid is mandatory.
 *  - If timestamp is given, for each pid we find the single doc whose 'ts' is closest to that timestamp (in seconds).
 *  - Otherwise, we fetch all documents (optional date range) and group them by pid, each sorted by ts asc.
 */
async function getCoinsTimeseries({ pid, startDate, endDate, timestamp }) {
  const client = getClient();
  if (!client) {
    throw new Error('No Elasticsearch client available.');
  }

  const pids = pid
    .split(',')
    .map((x) => x.trim().toLowerCase())
    .filter(Boolean);

  if (!pids.length) {
    throw new Error("No valid 'pid' provided.");
  }

  if (timestamp) {
    const timestampNum = parseInt(timestamp, 10) * 1000;
    if (isNaN(timestampNum)) {
      throw new Error('Invalid timestamp format. It should be a number (seconds).');
    }

    const results = {};
    for (const singlePid of pids) {
      const doc = await findClosestDocForPid(client, singlePid, timestampNum);
      results[singlePid] = doc;
    }
    return results;
  }

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
    query: {
      bool: {
        must: mustQueries
      }
    },
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

async function getAllMetadata() {
  const client = getClient();
  if (!client) {
    throw new Error('No ES client available.');
  }

  const pageSize = 10000;
  const allDocs = [];

  let response = await client.search({
    index: 'coins-metadata',
    scroll: '1m',
    size: pageSize,
    body: { query: { match_all: {} } }
  });

  while (response.hits && response.hits.hits.length > 0) {
    allDocs.push(...response.hits.hits.map((hit) => hit._source));

    response = await client.scroll({
      scroll_id: response._scroll_id,
      scroll: '1m'
    });
  }

  return allDocs;
}

async function refreshLocalMetadataFromES() {
  const allData = await getAllMetadata();
  console.log(`Fetched ${allData.length} metadata documents from ES.`);

  const newCache = {};
  for (const item of allData) {
    if (item.pid) {
      newCache[item.pid.toLowerCase()] = item;
    }
  }
  localMetadataCache = newCache;
  saveLocalMetadataCache();
}

module.exports = {
  getCoinMetadata,
  getCoinsService,
  getCoinsTimeseries,
  refreshLocalMetadataFromES
};
