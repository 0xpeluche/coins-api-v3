const coinsService = require('../services/coinsService');

async function getCoinsCurrentController(query) {
  const { pid, withMetadata = "false", withTTL = "false" } = query;
  if (!pid || typeof pid !== "string") {
    throw new Error("The 'pids' query parameter is required and must be a string.");
  }
  const pidArray = pid.split(",")
    .map(pid => pid.trim().toLowerCase())
    .filter(pid => pid);
  if (pidArray.length === 0) {
    throw new Error("The 'pid' query parameter must contain at least one valid pid.");
  }
  const includeTTL = withTTL === "true" || withTTL === true;
  const includeMetadata = withMetadata === "true" || withMetadata === true;
  const result = await coinsService.getCoinsService(pidArray, { withTTL: includeTTL, withMetadata: includeMetadata });
  return result;
}

async function getCoinsMetadataController(query) {
  return await coinsService.getCoinMetadata(query);
}

async function getCoinsTimeseriesController(query) {
  return await coinsService.getCoinsTimeseries(query);
}

module.exports = {
  getCoinsCurrentController,
  getCoinsMetadataController,
  getCoinsTimeseriesController,
};
