const { getClient } = require('../../db/elastic');
const { getCoinMetadata } = require('./getMetadataCoin')
const { getEffectivePids, parsePidMapping } = require('../../utils/index')

function filterTimeseriesDoc(doc) {
  if (!doc) return null;
  return {
    pid: doc.pid,
    ts: doc.ts,
    price: doc.price,
    confidence: doc.confidence
  };
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
async function getTimeseries({ pid, startDate, endDate, timestamp, scale }) {
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
    const { address, symbol, decimals, chain } = metadata[originalPid]
    coins[originalPid] = {
      pid: originalPid,
      address,
      symbol,
      chain,
      decimals,
      timeseries: timeseriesData[originalPid] || []
    };
  }
  
  return { coins };
}

module.exports = { getTimeseries }