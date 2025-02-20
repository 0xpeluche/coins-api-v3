
const fs = require('fs')
const path = require('path')
const { getProducer } = require('../db/kafka')
const { normalizeCoinId } = require('../utils')


// Paths
const cacheFolderPath = path.join(__dirname, '../.cache')
const metadataFile = fs.readFileSync(path.join(cacheFolderPath, 'metadataRecords.json'), 'utf8')
const metadataRecords = JSON.parse(metadataFile)
metadataRecords.forEach(record => {
  const pid = normalizeCoinId(record.PK)
  if (Array.isArray(record.redirects)) record.redirects = record.redirects.map(normalizeCoinId)
  record.pid = pid
  delete record.PK
  delete record.SK
  delete record.created
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
})
console.log('Loaded metadata records:', metadataRecords.length)


async function run() {
  const producer = await getProducer()
  const chunkSize = 1000
  const chunkCount = Math.ceil(metadataRecords.length / chunkSize)
  for (let i = 0; i < chunkCount; i++) {
    const chunk = metadataRecords.slice(i * chunkSize, (i + 1) * chunkSize).map(i => ({ value: JSON.stringify(i) }))
    await producer.send({
      topic: 'coins-metadata-test',
      messages: chunk,
    })
    console.log('Sent chunk:', i + 1 + '/' + chunkCount)
  }
}

run().catch((error) => {
  console.error('Error:', error)
}).then(() => {
  console.log('Done')
  process.exit(0)
})