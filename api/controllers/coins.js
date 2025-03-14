const { getCurrentCoin } = require('../services/getCurrentCoin')
const { getCoinsEarliest } = require('../services/getEarliestCoin')
const { getCoinMetadata } = require('../services/getMetadataCoin')
const { getTimeseries } = require('../services/getTimeseriesCoin')
const { getPercentageChange } = require('../services/getPercentageChangeCoin')

function sendResponse(res, status, data) {
  res.statusCode = status;
  res.header('Content-Type', 'application/json');
  res.send(JSON.stringify(data));
}

/**
 * GET /metadata
 * Example: /api/coins/metadata?pid=bitcoin,ethereum
 * This endpoint retrieves metadata from the local cache or Elasticsearch for the given pid(s).
 */
async function getCoinsMetadata(req, res) {
  try {
    const { pid } = req.query;
    if (!pid) {
      return sendResponse(res, 400, { error: "Missing 'pid' query parameter." });
    }
    const data = getCoinMetadata({ pid });
    return sendResponse(res, 200, data);
  } catch (error) {
    return sendResponse(res, 400, { error: error.message || 'Internal Server Error' });
  }
}

/**
 * GET /current
 * Example: /api/coins/current?pid=bitcoin,ethereum&withMetadata=true&withTTL=true
 * This endpoint retrieves current coin data from Redis for the given pid(s).
 * It can also include metadata or TTL if specified in the query.
 */
async function getCoinsCurrent(req, res) {
  try {
    const { pid, withTTL = 'false' } = req.query;
    if (!pid) {
      return sendResponse(res, 400, { error: "Missing 'pid' query parameter." });
    }
    const includeTTL = (withTTL === 'true');
    const data = await getCurrentCoin(pid, { withTTL: includeTTL });
    return sendResponse(res, 200, data);
  } catch (error) {
    return sendResponse(res, 400, { error: error.message || 'Internal Server Error' });
  }
}

/**
 * GET /timeseries
 * Example: /api/coins/timeseries?pid=bitcoin&startDate=2023-01-01&endDate=2023-01-31
 * Or:      /api/coins/timeseries?pid=bitcoin,ethereum&timestamp=1673000000
 *
 * pid is required.
 * If 'timestamp' is provided, for each pid we return the single document whose 'ts' is closest to that timestamp.
 * If 'timestamp' is not provided, we fetch all documents (optionally constrained by startDate/endDate), grouped by pid and sorted by ts ascending.
 */
async function getCoinsTimeseries(req, res) {
  try {
    const { pid, startDate, endDate, timestamp, scale } = req.query;
    if (!pid) {
      return sendResponse(res, 400, { error: "Missing 'pid' query parameter." });
    }
    const data = await getTimeseries({ pid, startDate, endDate, timestamp, scale });
    return sendResponse(res, 200, data);
  } catch (error) {
    return sendResponse(res, 400, { error: error.message || 'Internal Server Error' });
  }
}

/**
 * GET /earliest
 * Example: /api/coins/earliest?pid=bitcoin,ethereum
 * Returns, for each coin, the earliest record (i.e. with the lowest timestamp).
 */
async function getCoinsFirstTimestamp(req, res) {
  try {
    const { pid } = req.query;
    if (!pid) {
      return sendResponse(res, 400, { error: "Missing 'pid' query parameter." });
    }
    const data = await getCoinsEarliest({ pid });
    return sendResponse(res, 200, data);
  } catch (error) {
    return sendResponse(res, 400, { error: error.message || 'Internal Server Error' });
  }
}

/**
 * GET /percentage-change
 * Example: /api/coins/percentage-change?pid=bitcoin,ethereum&timestamp=1656944730&period=3600&lookForward=true
 * Parameters:
 * - pid: list of coins.
 * - timestamp: a reference timestamp in seconds.
 * - period: period (in seconds) to calculate the change.
 * - lookForward: if true, change is from t0 to t0 + period; otherwise from t0 to t0 - period.
 */
async function getCoinsPercentageChange(req, res) {
  try {
    const { pid, timestamp, period, lookForward } = req.query;
    if (!pid || !timestamp || !period) {
      return sendResponse(res, 400, { error: "Missing required query parameters: pid, timestamp, period." });
    }
    const data = await getPercentageChange({ pid, timestamp, period, lookForward });
    return sendResponse(res, 200, data);
  } catch (error) {
    return sendResponse(res, 400, { error: error.message || 'Internal Server Error' });
  }
}

module.exports = {
  getCoinsMetadata,
  getCoinsCurrent,
  getCoinsTimeseries,
  getCoinsFirstTimestamp,
  getCoinsPercentageChange
};