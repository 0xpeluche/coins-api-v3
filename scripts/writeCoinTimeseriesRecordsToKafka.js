const zlib = require('zlib')
const readline = require('readline')
const fs = require('fs')
const path = require('path')
const { PromisePool } = require('@supercharge/promise-pool')


// Paths
const manifestPath = path.join(__dirname, '../.cache/manifest.json')
const cacheFolderPath = path.join(__dirname, '../.cache')
const { unmarshall } = require('@aws-sdk/util-dynamodb')
const { normalizeCoinId, sleep } = require('../utils')
const { getProducer } = require('../db/kafka')
const logData = fs.readFileSync(__dirname+'/../runLog.log', 'utf8');

const processedFileSet = new Set();

try {
  const regex = /Processing file: (.*): .* \(/g;
  
  let match;
  while ((match = regex.exec(logData)) !== null) {
    processedFileSet.add(match[1]);
  }
} catch (error) {
  console.error('Error:', error)  
}


console.log('Processed file count:', processedFileSet.size)

let manifest = fs.readFileSync(manifestPath, 'utf8')
  .split('\n')
  .filter(line => line.trim() !== '')
  .map(line => JSON.parse(line))

const blacklistedTokenSet = new Set([
  '0xdb1d3cfaceafedc738563dc9c43b1304ad196d04',
])

// Promisified function to read and process gzipped file
async function readGzippedFile(file) {
  const fileName = path.basename(file.dataFileS3Key)
  const filePath = path.join(cacheFolderPath, fileName)
  // console.log(`Processing file: ${fileName}`)

  const fileStream = fs.createReadStream(filePath)
  const gzipStream = zlib.createGunzip()
  const rl = readline.createInterface({
    input: fileStream.pipe(gzipStream),
    crlfDelay: Infinity
  })

  let lineCount = 0
  for await (const line of rl) {
    // Each line in the readline input will be successively available here as
    // `line`.
    await processLine(line)
  }

  async function processLine(line) {
    lineCount++

    let json = unmarshall(JSON.parse(line).Item)
    const time = json.SK
    if (!time || time === 0) return; // we care about only historical data
    if (json.redirect || !json.PK || !json.price) return; // we don't care about redirects

    json.pid = normalizeCoinId(json.PK)
    if (blacklistedTokenSet.has(json.pid)) return;


    delete json.SK
    delete json.PK
    json.ts = Math.round(time / 300) * 300 // round to 5 minutes

    if (typeof json.price === 'bigint') json.price = Number(json.price)

    try {
      JSON.stringify(json)
    } catch (error) {
      console.error('Error stringifying json:', error)
      console.log('json:', json)
    }
    await writeToKafka(json)
  }
}

let writeQueue = []
let producer
let messageCount = 0
let lastMessageCount = 0
let lastMessageCountTime = Date.now()

async function writeToKafka(message) {
  writeQueue.push(message)

  if (writeQueue.length > 10000) {
    const tmpQueue = writeQueue
    writeQueue = []
    await writeQueueToFile(tmpQueue)
    messageCount += tmpQueue.length

    if (messageCount - lastMessageCount > 5e5) {
      const currentTime = Date.now()
      console.log('Sent messages:', Number(messageCount / 1e6).toFixed(3), 'M', 'Time taken:', (currentTime - lastMessageCountTime) / 1000, 'seconds', 'Messages per second:', (messageCount - lastMessageCount) / ((currentTime - lastMessageCountTime) / 1000))
      lastMessageCountTime = currentTime
      lastMessageCount = messageCount
    }
  }
}

async function writeQueueToFile(messages) {
  const chunkSize = 1000
  const chunkCount = Math.ceil(messages.length / chunkSize)
  for (let i = 0; i < chunkCount; i++) {
    const chunk = messages.slice(i * chunkSize, (i + 1) * chunkSize).map(i => ({ value: JSON.stringify(i) }))
    await producer.send({ topic: 'coins-timeseries', messages: chunk, })
  }
}


async function run() {
  producer = await getProducer()
  await PromisePool
    .withConcurrency(1)
    .for(manifest)
    .process(async (file, i) => {
      if (processedFileSet.has(file.dataFileS3Key)) {
        console.log('Skipping file:', file.dataFileS3Key)
        messageCount += file.itemCount
        return
      }
      console.log('Processing file:', file.dataFileS3Key, i + '/' + manifest.length, 'items:', file.itemCount)
      console.time('Processing file: ' + file.dataFileS3Key)
      try {
        await readGzippedFile(file)
      } catch (error) {
        console.error('Error processing file:', error)
      }
      console.log('Processed file:', i + '/' + manifest.length, 'items:', file.itemCount)
      console.timeEnd('Processing file: ' + file.dataFileS3Key)
      await sleep(15 * 60 * 1000) // sleep for 5 minutes
    })
  // write the missing records
  await writeQueueToFile(writeQueue)
  console.log('All files processed!')
}


run().catch((error) => {
  console.error('Error:', error)
}).then(() => {
  console.log('Done')
  process.exit(0)
})
