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
      metadataMap[originalPid] = {};
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

    if (!earliestDoc) {
      coins[originalPid] = { pid: originalPid };
    } else {
      const { ts: timestamp, price } = earliestDoc;
      coins[originalPid] = {
        pid: originalPid,
        address: metadataMap[originalPid].address,
        symbol: metadataMap[originalPid].symbol,
        chain: metadataMap[originalPid].chain,
        decimals: metadataMap[originalPid].decimals,
        timestamp,
        price
      };
    }
  }
  return { coins };
}

module.exports = { getCoinsEarliest }