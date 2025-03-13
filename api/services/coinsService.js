const { getMultipleKeyDetails } = require('../../db/redis');
const { getClient } = require('../../db/elastic');
const { normalizeCoinId } = require('../../utils');
const { initCacheWatcher, getLocalCache } = require('../cache/metadataCache');

initCacheWatcher();

function parsePidMapping(pidInput) {
  let inputArray = [];
  if (typeof pidInput === 'string') {
    inputArray = pidInput.split(',').map(p => p.trim()).filter(Boolean);
  } else if (Array.isArray(pidInput)) {
    inputArray = pidInput.map(p => p.trim()).filter(Boolean);
  }
  if (inputArray.length > 100) {
    throw new Error("Maximum of 100 PID tokens allowed.");
  }
  const mapping = {};
  const normalizedPids = inputArray.map(pid => {
    const normalized = normalizeCoinId(pid);
    mapping[pid] = normalized;
    return normalized;
  });
  return { mapping, normalizedPids };
}

/**
 * Retrieve coin metadata from the local cache.
 * Returns an object mapping the original PID (input) to its metadata.
 * If metadata for a normalized PID is missing, the value is null.
 */
function getCoinMetadata(query) {
  const { mapping, normalizedPids } = parsePidMapping(query.pid);
  if (!normalizedPids.length) {
    throw new Error("Parameter 'pid' is required for metadata.");
  }
  const localCache = getLocalCache();
  const resultMap = {};
  for (const originalPid in mapping) {
    const normalized = mapping[originalPid];
    resultMap[originalPid] = localCache[normalized] || null;
  }
  return resultMap;
}

/**
 * Retrieve the current coin data from Redis.
 * If withMetadata=true, attach metadata using getCoinMetadata.
 * Returns an object mapping the original PID to its coin data.
 */
async function getCoinsService(pidString, options = {}) {
  const { withTTL = false, withMetadata = false } = options;
  const { mapping, normalizedPids } = parsePidMapping(pidString);
  if (!normalizedPids.length) {
    throw new Error("The 'pid' query parameter must contain at least one valid pid.");
  }
  const redisKeys = normalizedPids.map(pid => `price_${pid}`);
  const coinsData = await getMultipleKeyDetails(redisKeys, withTTL);
  
  let metadataMap = {};
  if (withMetadata) {
    metadataMap = getCoinMetadata({ pid: pidString });
  }
  
  const coins = {};
  for (const originalPid in mapping) {
    const normalized = mapping[originalPid];
    const redisKey = `price_${normalized}`;
    const data = coinsData[redisKey] || { value: {}, ttl: null };
    const { pid: storedPid, price, confidence, source } = data.value;
    coins[originalPid] = {
      pid: storedPid || originalPid,
      price,
      confidence,
      source,
      ttl: withTTL ? data.ttl : undefined,
      metadata: withMetadata ? (metadataMap[originalPid] || null) : undefined
    };
  }
  
  return { coins };
}

async function findClosestDocForPid(client, pid, timestampMs) {
  const query = {
    index: 'coins-timeseries-*',
    size: 1,
    body: {
      query: {
        bool: {
          must: [
            { term: { pid } }
          ]
        }
      },
      sort: [
        {
          _script: {
            type: "number",
            script: {
              lang: "painless",
              source: "Math.abs(doc['ts'].value.toInstant().toEpochMilli() - params.target)",
              params: { target: timestampMs }
            },
            order: "asc"
          }
        }
      ]
    }
  };

  const response = await client.search(query);
  return response.hits?.hits[0]?._source || null;
}

/**
 * Retrieve timeseries data:
 * - If a timestamp (in seconds) is provided, for each PID return the document whose ts is closest.
 * - Otherwise, if startDate/endDate are provided as date strings (e.g., "2024-06-16"),
 *   convert them to UTC (start: T00:00:00Z, end: T23:59:59Z), convert to ms,
 *   perform a range query and group the matching documents by the original PID.
 */
function scaleToMillis(scale) {
  const match = scale.match(/^(\d+)([mhd])$/);
  if (!match) {
    throw new Error("Invalid scale format. Use e.g., '1m', '5m', '1h', or '1d'.");
  }
  const value = parseInt(match[1], 10);
  const unit = match[2];
  switch (unit) {
    case 'm':
      return value * 60 * 1000;
    case 'h':
      return value * 60 * 60 * 1000;
    case 'd':
      return value * 24 * 60 * 60 * 1000;
    default:
      throw new Error("Unsupported time unit in scale.");
  }
}

async function getCoinsTimeseries({ pid, startDate, endDate, timestamp, scale }) {
  const client = getClient();
  if (!client) {
    throw new Error('No Elasticsearch client available.');
  }
  const { mapping, normalizedPids } = parsePidMapping(pid);
  if (!normalizedPids.length) {
    throw new Error("No valid 'pid' provided.");
  }
  
  // Case 1: Timestamp mode
  if (timestamp) {
    const timestampMs = parseInt(timestamp, 10) * 1000;
    if (isNaN(timestampMs)) {
      throw new Error('Invalid timestamp format. It should be an integer in seconds.');
    }
    const results = {};
    for (const originalPid in mapping) {
      const normalized = mapping[originalPid];
      const doc = await findClosestDocForPid(client, normalized, timestampMs);
      results[originalPid] = doc;
    }
    return results;
  }
  
  // Case 2: Scale aggregation mode
  if (scale) {
    if (!startDate || !endDate) {
      throw new Error("startDate and endDate are required when scale is provided.");
    }
    const startMs = new Date(startDate + "T00:00:00Z").getTime();
    const endMs = new Date(endDate + "T23:59:59Z").getTime();
    
    // Convert scale to milliseconds and calculate expected buckets.
    const scaleMs = scaleToMillis(scale);
    const expectedBuckets = Math.ceil((endMs - startMs) / scaleMs);
    const maxBuckets = 10000;
    if (expectedBuckets > maxBuckets) {
      throw new Error(`Too many buckets expected (${expectedBuckets}, limit: ${maxBuckets}). Please use a larger scale.`);
    }
    
    const queryBody = {
      query: {
        bool: {
          must: [
            { range: { ts: { gte: startMs, lte: endMs } } },
            { terms: { pid: normalizedPids } }
          ]
        }
      },
      aggs: {
        by_pid: {
          terms: {
            field: "pid",
            size: normalizedPids.length
          },
          aggs: {
            by_interval: {
              date_histogram: {
                field: "ts",
                fixed_interval: scale, // e.g., "1m", "5m", "1h", "1d"
                min_doc_count: 0,
                extended_bounds: {
                  min: startMs,
                  max: endMs
                }
              },
              aggs: {
                avg_price: { avg: { field: "price" } },
                min_price: { min: { field: "price" } },
                max_price: { max: { field: "price" } }
              }
            }
          }
        }
      },
      size: 0
    };

    const resp = await client.search({
      index: 'coins-timeseries-*',
      body: queryBody
    });
    
    const buckets = resp.aggregations?.by_pid?.buckets || [];
    const results = {};
    for (const bucket of buckets) {
      const normPid = bucket.key;
      const originalPid = Object.keys(mapping).find(key => mapping[key] === normPid) || normPid;
      results[originalPid] = bucket.by_interval.buckets.map(b => ({
          timestamp: b.key / 1000,
          avg_price: b.avg_price.value,
          min_price: b.min_price.value,
          max_price: b.max_price.value,
          count: b.doc_count,
      }));
    }
    return results;
  }
  
  // Case 3: Standard range query (without aggregation)
  let pidQuery = normalizedPids.length === 1
    ? { term: { pid: normalizedPids[0] } }
    : { terms: { pid: normalizedPids } };
  const mustQueries = [pidQuery];
  const tsRange = {};
  if (startDate) {
    tsRange.gte = new Date(startDate + "T00:00:00Z").getTime();
  }
  if (endDate) {
    tsRange.lte = new Date(endDate + "T23:59:59Z").getTime();
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
    const originalPid = Object.keys(mapping).find(key => mapping[key] === source.pid) || source.pid;
    if (!grouped[originalPid]) {
      grouped[originalPid] = [];
    }
    grouped[originalPid].push(source);
  }
  for (const p in grouped) {
    grouped[p].sort((a, b) => a.ts - b.ts);
  }
  return grouped;
}

module.exports = {
  getCoinMetadata,
  getCoinsService,
  getCoinsTimeseries,
};
