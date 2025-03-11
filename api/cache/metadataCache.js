const fs = require('fs');
const path = require('path');
const { getClient } = require('../../db/elastic');

let localMetadataCache = {};

const DATA_DIR = path.join(__dirname, '../data');
const METADATA_JSON_PATH = path.join(DATA_DIR, 'metadata.json');

function ensureCacheFileExists() {
  if (!fs.existsSync(DATA_DIR)) {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    console.log("Data directory created:", DATA_DIR);
  }
  if (!fs.existsSync(METADATA_JSON_PATH)) {
    fs.writeFileSync(METADATA_JSON_PATH, '{}', 'utf8');
    console.log("Created empty metadata file:", METADATA_JSON_PATH);
  }
}

function loadLocalMetadataCache() {
  try {
    ensureCacheFileExists();
    const data = fs.readFileSync(METADATA_JSON_PATH, 'utf8');
    localMetadataCache = JSON.parse(data);
    console.log("Local metadata cache reloaded.");
  } catch (err) {
    console.error("Error loading metadata cache file:", err);
    localMetadataCache = {};
  }
}

function saveLocalMetadataCache() {
  ensureCacheFileExists();
  fs.writeFile(
    METADATA_JSON_PATH,
    JSON.stringify(localMetadataCache, null, 2),
    'utf8',
    (err) => {
      if (err) {
        console.error("Failed to write local metadata cache file:", err);
      } else {
        console.log(`Cache saved to ${METADATA_JSON_PATH}`);
      }
    }
  );
}


function initCacheWatcher() {
  loadLocalMetadataCache();

  fs.watchFile(METADATA_JSON_PATH, { interval: 1000 }, (curr, prev) => {
    if (curr.mtimeMs !== prev.mtimeMs) {
      console.log("Detected change in metadata file, reloading cache...");
      loadLocalMetadataCache();
    }
  });
}

function getLocalCache() {
  return localMetadataCache;
}

function updateLocalCache(newCache) {
  localMetadataCache = newCache;
  saveLocalMetadataCache();
}

async function getAllMetadata() {
  const client = getClient();
  if (!client) {
    throw new Error('No Elasticsearch client available.');
  }

  const pageSize = 100000;
  const allDocs = [];

  let response = await client.search({
    index: 'coins-metadata',
    scroll: '1m',
    size: pageSize,
    body: { query: { match_all: {} } }
  });

  while (response.hits && response.hits.hits.length > 0) {
    allDocs.push(...response.hits.hits.map(hit => hit._source));
    response = await client.scroll({
      scroll_id: response._scroll_id,
      scroll: '1m'
    });
  }
  return allDocs;
}

async function refreshLocalMetadataFromES() {
  const allData = await getAllMetadata();
  console.log(`Fetched ${allData.length} metadata documents from ES.`);
  
  const newCache = { ...localMetadataCache };
  for (const item of allData) {
    if (item.pid) {
      newCache[item.pid.toLowerCase()] = item;
    }
  }
  updateLocalCache(newCache);
}

module.exports = {
  initCacheWatcher,
  getLocalCache,
  updateLocalCache,
  refreshLocalMetadataFromES
};
