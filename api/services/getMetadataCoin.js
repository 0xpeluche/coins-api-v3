const { initCacheWatcher, getLocalCache } = require('../cache/metadataCache');
const { parsePidMapping } = require('../../utils/index')

initCacheWatcher();

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

module.exports = { getCoinMetadata }