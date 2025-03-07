const { getMultipleKeyDetails } = require("../../db/redis");
const { getMetadataForPids, getClient } = require("../../db/elastic");

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
      metadata: withMetadata ? (metadataMap[pid] || null) : undefined
    };
  });

  return { coins };
}

async function getAllMetadata() {
  const client = getClient();
  const result = await client.search({
    index: 'coins-metadata',
    body: { query: { match_all: {} } }
  });
  return result.hits.hits.map(hit => hit._source);
}

async function getCoinsTimeseries(query) {
  let { pid, startDate, endDate } = query;

  if (!pid && !startDate) {
    throw new Error("Either 'pid' or 'startDate' must be provided.");
  }
  if (!endDate) {
    endDate = new Date().toISOString().slice(0, 10);
  }

  const client = getClient();
  if (pid) {
    const pidArray = pid.split(",").map(p => p.trim().toLowerCase()).filter(p => p);
    let pidQuery;
    if (pidArray.length === 1) {
      pidQuery = { term: { pid: pidArray[0] } };
    } else {
      pidQuery = { terms: { pid: pidArray } };
    }
    let queryBody = {
      bool: {
        must: [ pidQuery ]
      }
    };
    if (startDate) {
      queryBody.bool.must.push({
        range: {
          ts: {
            gte: Math.floor(new Date(startDate).getTime() / 1000),
            lte: Math.floor(new Date(endDate).getTime() / 1000)
          }
        }
      });
    }
    const result = await client.search({
      index: 'coins-timeseries-*',
      body: {
        query: queryBody,
        sort: [{ ts: { order: "asc" } }],
        size: 10000
      }
    });
    return result.hits.hits.map(hit => hit._source);
  } else {
    const result = await client.search({
      index: 'coins-timeseries-*',
      body: {
        query: {
          range: {
            ts: {
              gte: Math.floor(new Date(startDate).getTime() / 1000),
              lte: Math.floor(new Date(endDate).getTime() / 1000)
            }
          }
        },
        sort: [{ ts: { order: "asc" } }],
        size: 10000
      }
    });
    return result.hits.hits.map(hit => hit._source);
  }
}

async function getCoinMetadata(query) {
  let { pid } = query;
  if (!pid || typeof pid !== "string") {
    throw new Error("The 'pid' query parameter is required for metadata.");
  }
  const pidArray = pid.split(",").map(p => p.trim().toLowerCase()).filter(p => p);
  const metadataMap = await getMetadataForPids(pidArray);
  if (Object.keys(metadataMap).length === 0) {
    throw new Error("Metadata not found for the given pid(s).");
  }
  return metadataMap;
}

module.exports = {
  getCoinsService,
  getAllMetadata,
  getCoinsTimeseries,
  getCoinMetadata,
};
