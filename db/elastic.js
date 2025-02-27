const { Client } = require('@elastic/elasticsearch')

let _client
function getClient() {
  if (_client) return _client
  let config
  try {
    const envString = process.env['COINS_ELASTICSEARCH_CONFIG']
    if (!envString) return;
    config = JSON.parse(envString.replace(/\\"/g, '"')) // replace escaped quotes
  } catch (error) {
    return;
  }
  if (!_client)
    _client = new Client({
      maxRetries: 3,
      requestTimeout: 5000,
      compression: true,
      node: config.host,
      auth: {
        username: config.username,
        password: config.password,
      },
      tls: {
        rejectUnauthorized: false,
      },
    })
  return _client
}

async function writeLog(index, log) {
  const client = getClient()
  if (!client) return;
  index = addYearAndMonth(index)
  log.timestamp = +Date.now()
  try {
    await client.index({ index, body: log })
  } catch (error) {
    console.error(error)
  }
}

function addYearAndMonth(index) {
  const date = new Date()
  return `${index}-${date.getUTCFullYear()}-${date.getUTCMonth()}`
}

async function getMetadataForPids(pids) {
  const client = getClient();
  if (!client) {
    console.error('No Elasticsearch client available.');
    return {};
  }

  try {
    const result = await client.search({
      index: 'coins-metadata',
      size: pids.length,
      body: {
        query: {
          bool: {
            should: pids.map(pid => ({ term: { pid } })),
            minimum_should_match: 1
          }
        }
      }
    });

    const metadataMap = {};
    if (result.hits && result.hits.hits.length) {
      for (const hit of result.hits.hits) {
        const src = hit._source;
        if (src && src.pid) {
          metadataMap[src.pid] = src;
        }
      }
    }

    return metadataMap;
  } catch (error) {
    console.error('Error in getMetadataForPids:', error);
    return {};
  }
}

module.exports = { getClient, writeLog, getMetadataForPids }