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

module.exports = { getPercentageChange }