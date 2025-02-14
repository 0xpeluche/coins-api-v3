
const fs = require('fs')
const path = require('path')
const { getProducer } = require('../db/kafka')


// Paths
const cacheFolderPath = path.join(__dirname, '../.cache')
const metadataFile = fs.readFileSync(path.join(cacheFolderPath, 'metadataRecords.json'), 'utf8')
const metadataRecords = JSON.parse(metadataFile)
metadataRecords.forEach(record => {
  const pid = record.PK
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

console.table(metadataRecords.slice(0, 100))


async function run() {
  const producer = await getProducer()
  const chunkCount = Math.ceil(metadataRecords.length / 100)
  for (let i = 0; i < chunkCount; i++) {
    const chunk = metadataRecords.slice(i * 100, (i + 1) * 100).map(i => ({ value: JSON.stringify(i) }))
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