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

function getEffectivePids(originalPid, mapping, metadata) {
  const normalized = mapping[originalPid];
  let candidates = [];
  const meta = metadata[originalPid];
  if (meta && Array.isArray(meta.redirects) && meta.redirects.length > 0) {
    candidates = [...meta.redirects];
  }
  candidates.push(normalized);
  return [...new Set(candidates)];
}

function filterTimeseriesDoc(doc) {
  if (!doc) return null;
  return {
    pid: doc.pid,
    ts: doc.ts,
    price: doc.price,
    confidence: doc.confidence
  };
}

function filterMetadata(meta) {
  if (!meta) return null;
  return {
    pid: meta.pid,
    address: meta.address,
    symbol: meta.symbol,
    decimals: meta.decimals,
    chain: meta.chain,
    redirects: meta.redirects
  };
}

/**
 * Retrieve coin metadata from the local cache.
 * Returns an object mapping the original PID (input) to its filtered metadata.
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
    resultMap[originalPid] = filterMetadata(localCache[normalized]) || null;
  }
  return resultMap;
}

/**
 * Retrieve the current coin data from Redis.
 * Always attach metadata using getCoinMetadata.
 * Returns an object mapping the original PID to its coin data.
 */
async function getCoinsService(pidString, options = {}) {
  const { withTTL = false } = options;
  const { mapping, normalizedPids } = parsePidMapping(pidString);
  if (!normalizedPids.length) {
    throw new Error("The 'pid' query parameter must contain at least one valid pid.");
  }
  const redisKeys = normalizedPids.map(pid => `price_${pid}`);
  const coinsData = await getMultipleKeyDetails(redisKeys, withTTL);
  const metadataMap = getCoinMetadata({ pid: pidString });

  for (const originalPid in mapping) {
    if (!metadataMap[originalPid]) {
      throw new Error(`Missing metadata for coin ${originalPid}`);
    }
  }
  
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

async function findClosestDocForPid(client, pid, timestampMs) {
  const query = {
    index: 'coins-timeseries-*',
    size: 1,
    body: {
      query: {
        bool: {
          must: [{ term: { pid } }]
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
  return filterTimeseriesDoc(response.hits?.hits[0]?._source) || null;
}

/**
 * Convert a scale string (e.g., "1m", "5m", "1h", "1d") to milliseconds.
 */
function scaleToMillis(scale) {
  const match = scale.match(/^(\d+)([mhd])$/);
  if (!match) {
    throw new Error("Invalid scale format. Use e.g., '1m', '5m', '1h', or '1d'.");
  }
  const value = parseInt(match[1], 10);
  const unit = match[2];
  switch (unit) {
    case 'm': return value * 60 * 1000;
    case 'h': return value * 60 * 60 * 1000;
    case 'd': return value * 24 * 60 * 60 * 1000;
    default: throw new Error("Unsupported time unit in scale.");
  }
}

/**
 * Retrieve timeseries data:
 * - If a timestamp (in seconds) is provided, for each PID return the document whose ts is closest.
 * - Otherwise, if startDate/endDate are provided as date strings (e.g., "2024-06-16"),
 *   convert them to UTC (start: T00:00:00Z, end: T23:59:59Z), convert to ms,
 *   perform a range query and group the matching documents by the original PID.
 * Also, metadata is always attached.
 */
async function getCoinsTimeseries({ pid, startDate, endDate, timestamp, scale }) {
  const client = getClient();
  if (!client) {
    throw new Error('No Elasticsearch client available.');
  }
  
  const { mapping, normalizedPids } = parsePidMapping(pid);
  if (!normalizedPids.length) {
    throw new Error("No valid 'pid' provided.");
  }
  
  const metadata = getCoinMetadata({ pid });
  
  for (const originalPid in mapping) {
    if (!metadata[originalPid]) {
      throw new Error(`Missing metadata for coin ${originalPid}`);
    }
  }
  
  let timeseriesData;
  
  // 1. Mode Timestamp
  if (timestamp) {
    const timestampMs = parseInt(timestamp, 10) * 1000;
    if (isNaN(timestampMs)) {
      throw new Error('Invalid timestamp format. It should be an integer in seconds.');
    }
    const results = {};
    for (const originalPid in mapping) {
      const candidates = getEffectivePids(originalPid, mapping, metadata);
      let doc = null;
      for (const candidate of candidates) {
        doc = await findClosestDocForPid(client, candidate, timestampMs);
        if (doc) break;
      }
      results[originalPid] = doc;
    }
    timeseriesData = results;
  }
  // 2. Mode Scale aggregation
  else {
    if (!scale) {
      scale = "1h";
    }
    
    const now = new Date();
    if (!startDate && !endDate) {
      const yesterday = new Date(now);
      yesterday.setDate(now.getDate() - 1);
      startDate = yesterday.toISOString().slice(0,10);
      endDate = now.toISOString().slice(0,10);
    } else if (startDate && !endDate) {
      endDate = now.toISOString().slice(0,10);
    }
    
    const startMs = new Date(startDate + "T00:00:00Z").getTime();
    const endMs = new Date(endDate + "T23:59:59Z").getTime();
    
    const effectiveMap = {};
    const candidateToOriginal = {};
    for (const originalPid in mapping) {
      const eff = getEffectivePids(originalPid, mapping, metadata);
      effectiveMap[originalPid] = eff;
      eff.forEach(candidate => {
        candidateToOriginal[candidate] = originalPid;
      });
    }
    const unionCandidates = [...new Set(Object.values(effectiveMap).flat())];
    
    const scaleMs = scaleToMillis(scale);
    const expectedBuckets = Math.ceil((endMs - startMs) / scaleMs);
    const maxBuckets = 5000;
    if (expectedBuckets > maxBuckets) {
      throw new Error(`Too many buckets expected (${expectedBuckets}, limit: ${maxBuckets}). Please use a larger scale.`);
    }
    
    const queryBody = {
      query: {
        bool: {
          must: [
            { range: { ts: { gte: startMs, lte: endMs } } },
            { terms: { pid: unionCandidates } }
          ]
        }
      },
      aggs: {
        by_pid: {
          terms: {
            field: "pid",
            size: unionCandidates.length
          },
          aggs: {
            by_interval: {
              date_histogram: {
                field: "ts",
                fixed_interval: scale,
                min_doc_count: 0,
                extended_bounds: { min: startMs, max: endMs }
              },
              aggs: {
                avg_price: { avg: { field: "price" } },
                min_price: { min: { field: "price" } },
                max_price: { max: { field: "price" } },
                avg_confidence: { avg: { field: "confidence" } }
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
      const bucketPid = bucket.key;
      const originalPid = Object.keys(mapping).find(key => getEffectivePids(key, mapping, metadata)[0] === bucketPid) || bucketPid;
      results[originalPid] = bucket.by_interval.buckets.map(b => ({
        timestamp: b.key / 1000,
        avg_price: b.avg_price.value,
        min_price: b.min_price.value,
        max_price: b.max_price.value,
        confidence: b.avg_confidence.value,
        count: b.doc_count
      }));
    }
    for (const originalPid in mapping) {
      if (!results[originalPid]) results[originalPid] = [];
    }
    timeseriesData = results;
  }
  
  const coins = {};
  for (const originalPid in mapping) {
    const { pid, address, symbol, decimals, chain } = metadata[originalPid]
    coins[originalPid] = {
      pid: originalPid,
      address,
      symbol,
      chain,
      decimals,
      timeseries: {
        pid,
        series: timeseriesData[originalPid] || []
      }
    };
  }
  
  return { coins };
}


async function getEarliestRecord(client, pid) {
  const query = {
    index: 'coins-timeseries-*',
    size: 1,
    body: {
      query: { term: { pid } },
      sort: [{ ts: { order: 'asc' } }]
    }
  };
  const response = await client.search(query);
  return filterTimeseriesDoc(response.hits?.hits[0]?._source) || null;
}

/**
 * Retrieve, for each coin (by original PID), the earliest record along with metadata.
 */
async function getCoinsEarliest({ pid }) {
  const client = getClient();
  if (!client) {
    throw new Error('No Elasticsearch client available.');
  }
  const { mapping, normalizedPids } = parsePidMapping(pid);
  if (!normalizedPids.length) {
    throw new Error("No valid 'pid' provided.");
  }
  
  const metadataMap = getCoinMetadata({ pid });
  for (const originalPid in mapping) {
    if (!metadataMap[originalPid]) {
      throw new Error(`Missing metadata for coin ${originalPid}`);
    }
  }
  
  const coins = {};
  for (const originalPid in mapping) {
    const effectiveCandidates = getEffectivePids(originalPid, mapping, metadataMap);
    let earliestDoc = null;
    for (const candidate of effectiveCandidates) {
      earliestDoc = await getEarliestRecord(client, candidate);
      if (earliestDoc) break;
    }

    const { ts: timestamp, price } = earliestDoc
    const { address, symbol, decimals, chain } = metadataMap[originalPid]
    coins[originalPid] = {
      pid: originalPid,
      address,
      symbol,
      chain,
      decimals,
      timestamp,
      price
    };
  }
  return { coins };
}

/**
 * Retrieve percentage change in price between two timestamps.
 * Parameters:
 * - pid: list of coins.
 * - timestamp: reference timestamp in seconds.
 * - period: period in seconds to look forward (if lookForward is true) or backward.
 * - lookForward: if true, change is calculated from t0 to t0 + period, otherwise from t0 to t0 - period.
 */
async function getPercentageChange({ pid, timestamp, period, lookForward }) {
  const client = getClient();
  if (!client) {
    throw new Error('No Elasticsearch client available.');
  }
  const { mapping, normalizedPids } = parsePidMapping(pid);
  if (!normalizedPids.length) {
    throw new Error("No valid 'pid' provided.");
  }
  
  const t0 = parseInt(timestamp, 10) * 1000;
  if (isNaN(t0)) {
    throw new Error("Invalid timestamp format. It should be an integer in seconds.");
  }
  
  const periodSec = Number(period);
  if (isNaN(periodSec)) {
    throw new Error("Invalid period format. It should be a number representing seconds.");
  }
  
  const t1 = (lookForward === 'true' || lookForward === true)
    ? t0 + periodSec * 1000
    : t0 - periodSec * 1000;
  
  const metadataMap = getCoinMetadata({ pid });
  for (const originalPid in mapping) {
    if (!metadataMap[originalPid]) {
      throw new Error(`Missing metadata for coin ${originalPid}`);
    }
  }
  
  const coins = {};
  for (const originalPid in mapping) {
    const effectiveCandidates = getEffectivePids(originalPid, mapping, metadataMap);
    let doc0 = null, doc1 = null;
    for (const candidate of effectiveCandidates) {
      doc0 = await findClosestDocForPid(client, candidate, t0);
      if (doc0) break;
    }
    for (const candidate of effectiveCandidates) {
      doc1 = await findClosestDocForPid(client, candidate, t1);
      if (doc1) break;
    }

    const { address, symbol, decimals, chain } = metadataMap[originalPid]

    if (!doc0 || !doc1 || doc0.price === 0) {
      coins[originalPid] = {
        pid: originalPid,
        address,
        symbol,
        chain,
        decimals,
        percentageChange: null,
      };
    } else {
      const percentageChange = ((doc1.price - doc0.price) / doc0.price) * 100;
      coins[originalPid] = {
        pid: originalPid,
        address,
        symbol,
        chain,
        decimals,
        percentageChange,
      };
    }
  }
  return { coins };
}

module.exports = {
  getCoinMetadata,
  getCoinsService,
  getCoinsTimeseries,
  getCoinsEarliest,
  getPercentageChange
};
