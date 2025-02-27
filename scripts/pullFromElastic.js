const { getClient } = require("../db/elastic");
const { normalizeCoinId } = require("../utils");
const fs = require('fs')
const sdk = require('@defillama/sdk')

let isInitialized = false
let initializing

const metadataMap = {}
const redirectsMap = {}

const multiRedirects = []

async function init() {
  if (isInitialized) return;
  if (!initializing) initializing = _init()
  await initializing
  isInitialized = true;
}

const cacheFile = 'coins-v3/coinMetadataMap.json'
const expireAfter = 15 * 60  // 15 minutes in seconds

async function _init() {
  let records
  const currentCache = await sdk.cache.readExpiringJsonCache(cacheFile)

  if (currentCache) {
    records = currentCache
  } else {
    records = await fetchAllRecords('coins-metadata')
    // this is asynchrous, but we don't need to wait for it to finish
    sdk.cache.writeExpiringJsonCache(cacheFile, records, { expireAfter })
  }

  records.forEach(record => {
    if (!record.pid) {
      console.log('No pid:', record)
      return;
    }
    record.pid = normalizeCoinId(record.pid)
    if (Array.isArray(record.redirects)) record.redirects = record.redirects.map(normalizeCoinId)
    metadataMap[record.pid] = record
  })

  records.forEach(record => {
    if (!record.redirects) return;
    const allRedirects = []
    record.redirects.forEach(redirect => {
      if (!redirectsMap[redirect]) redirectsMap[redirect] = getRedirectChain(redirect)
      allRedirects.push(...redirectsMap[redirect])
    })

    record.allRedirects = [...new Set(allRedirects)]
    if (record.allRedirects.length > 1) multiRedirects.push({ pid: record.pid, symbol: record.symbol, allRedirects: record.allRedirects, count: record.allRedirects.length })
  })

  // console.table(multiRedirects)
  const redirectsCount = records.filter(i => i.redirects?.length).length
  console.log(`Fetched ${records.length} records, ${multiRedirects.length} with multiple redirects, ${redirectsCount} with redirects`)
}

function getRedirectChain(pid, chain = [], processedSet = new Set()) {
  if (processedSet.has(pid)) { // already processed, to catch circular redirects
    console.log('Circular redirect detected:', pid, chain)
    return chain;
  }
  if (!metadataMap[pid]) return chain;  // pid not in our db

  processedSet.add(pid)
  chain.push(pid)
  const record = metadataMap[pid]
  if (!record.redirects) return chain;  // no further redirects
  record.redirects.forEach(redirect => getRedirectChain(redirect, chain, processedSet))

  return chain;
}

async function fetchAllRecords(index) {
  const allRecords = [];
  const client = getClient();
  let response = await client.search({
    index: index,
    scroll: '1m',
    body: {
      query: {
        match_all: {}
      },
      size: 100000
    }
  });

  while (response.hits.hits.length) {
    allRecords.push(...response.hits.hits.map(i => {
      const source = i._source;
      // reduce final file size by removing fields we don't need
      // delete source.chain
      // delete source.address
      // delete source.adapter
      return source
    }));
    console.log(`Fetched ${allRecords.length} records`, response.hits.hits.length);
    response = await client.scroll({
      scroll_id: response._scroll_id,
      scroll: '1m'
    });
  }

  return allRecords;
}

const overrideMetadataFields = ['symbol', 'decimals', 'adapter']


// this returns a record if a record needs to be added/updated, otherwise returns undefined
function getMetadataRecord(json) {
  if (!initialized)
    throw new Error('Coin metadata cache not initialized')


  // probably a price record that does not contain any metadata info
  if (!json.decimals && !json.symbol) return;

  if (json.PK) // it is a dynamodb record, need to chang to elastic search record
    json = normalizeRecord(json)

  let existingRecord = metadataMap[record.pid]

  if (existingRecord) {
    let needsUpdate = false
    overrideMetadataFields.forEach(field => {
      if (json[field] && json[field] !== existingRecord[field]) {
        existingRecord[field] = json[field]
        needsUpdate = true
      }
    })
    if (Array.isArray(json.redirects) && json.redirects.length) {
      if (!existingRecord.redirects) existingRecord.redirects = []
      json.redirects.forEach(redirect => {
        if (!existingRecord.redirects.includes(redirect)) {
          existingRecord.redirects.push(redirect);
          needsUpdate = true
        }
      });
    }
    if (needsUpdate) {
      record = existingRecord
    } else {
      return;
    }
  } else {
    if (record.redirects) {
      let allRedirects = record.redirects.map(i => getRedirectChain(i))
      record.allRedirects = [...new Set(allRedirects.flat())]
    }
    metadataMap[record.pid] = record
  }

  const recordClone = { ...record }
  delete recordClone.allRedirects  // we dont want to store this field in the metadata record
  return recordClone
}

function normalizeRecord(record) {
  const pid = normalizeCoinId(record.PK)
  if (record.redirect) {
    record.redirects = [record.redirect]
    delete record.redirect
  }
  if (Array.isArray(record.redirects)) record.redirects = record.redirects.map(normalizeCoinId)
  record.pid = pid

  // reduce final record size by removing fields we don't need
  delete record.PK
  delete record.SK
  delete record.created
  delete record.price
  delete record.confidence

  if (pid.includes(':')) {
    record.chain = pid.split(':')[0]
    record.address = pid.slice(record.chain.length + 1)
  } else if (pid.length === 42 && pid.startsWith('0x')) {
    record.chain = 'ethereum'
    record.address = pid
  } else if (!record.decimals && !pid.startsWith('0x')) {
    record.chain = 'coingecko'
  }

  if (record.decimals) record.decimals = Number(record.decimals)
}


init().catch(err => console.error('Error fetching records:', err)).then(() => {
  console.log('Metadata cache initialized')
  process.exit(0)
});
