const zlib = require('zlib')
const readline = require('readline')
const fs = require('fs')
const path = require('path')


// Paths
const manifestPath = path.join(__dirname, '../.cache/manifest.json')
const cacheFolderPath = path.join(__dirname, '../.cache')
const { unmarshall } = require('@aws-sdk/util-dynamodb')
const { normalizeCoinId } = require('../utils')

let manifest = fs.readFileSync(manifestPath, 'utf8')
  .split('\n')
  .filter(line => line.trim() !== '')
  .map(line => JSON.parse(line))


// Promisified function to read and process gzipped file
async function readGzippedFile(file) {
  const fileName = path.basename(file.dataFileS3Key)
  const filePath = path.join(cacheFolderPath, fileName)
  console.log(`Processing file: ${fileName}`)

  const fileStream = fs.createReadStream(filePath)
  const gzipStream = zlib.createGunzip()
  const rl = readline.createInterface({
    input: fileStream.pipe(gzipStream),
    crlfDelay: Infinity
  })

  let lineCount = 0
  return new Promise((resolve, reject) => {

    rl.on('line', processLine)

    rl.on('close', () => {
      console.log('File processing completed. Total lines:', lineCount / 1e6, 'M')
      resolve()
    })

    rl.on('error', (err) => {
      reject(err)
    })
  })


  function processLine(line) {
    lineCount++
    let json = unmarshall(JSON.parse(line).Item)
    addOrUpdateMetadata(json)
  }
}

const metadataMap = {}

try {
  const metadataFile = fs.readFileSync(path.join(cacheFolderPath, 'metadataRecords.json'), 'utf8')
  const metadataRecords = JSON.parse(metadataFile)
  metadataRecords.forEach(record => {
    metadataMap[record.PK] = record
    if (record.redirects) record.redirects = new Set(record.redirects)
  })
  console.log('Loaded metadata records:', Object.keys(metadataMap).length)
} catch (error) {
  console.error('Error reading metadataFile:', error)
}


async function run() {
  let i = 0
  for (const entry of manifest) {
    i++
    try {
      await readGzippedFile(entry)
      writeMetadataRecordsToFile()
    } catch (error) {
      console.error('Error processing file:', error)
    }
    console.log('Processed file:', i + '/' + manifest.length)
  }
  console.log('All files processed!')
}

function writeMetadataRecordsToFile() {
  let metadataRecords = Object.values(metadataMap)
  console.log('Total metadata records:', metadataRecords.length)
  // to convert Set to Array
  metadataRecords = metadataRecords.map(record => {
    const { redirects, ...clone } = record
    if (!redirects) return clone
    return { ...clone, redirects: [...redirects] }
  })
  const metadataRecordsString = JSON.stringify(metadataRecords)
  fs.writeFileSync(path.join(cacheFolderPath, 'metadataRecords.json'), metadataRecordsString)
}

run().catch((error) => {
  console.error('Error:', error)
}).then(() => {
  console.log('Done')
  process.exit(0)
})



const overrideMetadataFields = ['symbol', 'decimals', 'created', 'adapter', 'SK', 'PK']

function addOrUpdateMetadata(json) {
  if (!json.decimals && !json.symbol) return
  let { redirect, SK, PK, } = json
  PK = normalizeCoinId(PK)
  if (redirect) redirect = normalizeCoinId(redirect)

  let existingRecord = metadataMap[PK]

  if (existingRecord) {
    if (SK === 0 || SK > existingRecord.SK) {
      overrideMetadataFields.forEach(field => existingRecord[field] = json[field])
    }
  } else {
    metadataMap[PK] = {}
    overrideMetadataFields.forEach(field => metadataMap[PK][field] = json[field])
  }

  existingRecord = metadataMap[PK]
  existingRecord.PK = PK

  if (redirect) {
    if (!existingRecord.redirects) existingRecord.redirects = new Set()
    existingRecord.redirects.add(redirect)
  }
}
