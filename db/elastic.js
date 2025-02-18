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

module.exports = { getClient, writeLog }