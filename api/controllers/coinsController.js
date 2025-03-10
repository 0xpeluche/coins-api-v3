const coinsService = require('../services/coinsService');

/**
 * GET /metadata
 * Example: /api/coins/metadata?pid=bitcoin,ethereum
 * This endpoint retrieves metadata from the local cache or Elasticsearch for the given pid(s).
 */
async function getCoinsMetadata(req, res) {
  try {
    const { pid } = req.query;
    if (!pid) {
      res.statusCode = 400;
      return res.send(JSON.stringify({ error: "Missing 'pid' query parameter." }));
    }

    const data = await coinsService.getCoinMetadata({ pid });
    res.header('Content-Type', 'application/json');
    return res.send(JSON.stringify(data));
  } catch (error) {
    res.statusCode = 400;
    return res.send(JSON.stringify({ error: error.message || 'Internal Server Error' }));
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
    const { pid, withMetadata = 'false', withTTL = 'false' } = req.query;

    if (!pid) {
      res.statusCode = 400;
      return res.send(JSON.stringify({ error: "Missing 'pid' query parameter." }));
    }

    const includeTTL = (withTTL === 'true');
    const includeMetadata = (withMetadata === 'true');

    const result = await coinsService.getCoinsService(pid, {
      withTTL: includeTTL,
      withMetadata: includeMetadata
    });

    res.header('Content-Type', 'application/json');
    return res.send(JSON.stringify(result));
  } catch (error) {
    res.statusCode = 400;
    return res.send(JSON.stringify({ error: error.message || 'Internal Server Error' }));
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
    const { pid, startDate, endDate, timestamp } = req.query;

    if (!pid) {
      res.statusCode = 400;
      return res.send(JSON.stringify({ error: "Missing 'pid' query parameter." }));
    }

    const data = await coinsService.getCoinsTimeseries({ pid, startDate, endDate, timestamp });

    res.header('Content-Type', 'application/json');
    return res.send(JSON.stringify(data));
  } catch (error) {
    res.statusCode = 400;
    return res.send(JSON.stringify({ error: error.message || 'Internal Server Error' }));
  }
}

module.exports = {
  getCoinsMetadata,
  getCoinsCurrent,
  getCoinsTimeseries
};
