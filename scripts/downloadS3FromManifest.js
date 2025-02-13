const fs = require('fs');
const path = require('path');
const ProgressBar = require('progress');
const { pipeline, Transform } = require('stream');
const { promisify } = require('util');
const { S3Client, GetObjectCommand, } = require('@aws-sdk/client-s3');

const s3 = new S3Client({
  region: "eu-central-1",
});

// Paths
const manifestPath = path.join(__dirname, '../.cache/manifest.json');
const cacheFolderPath = path.join(__dirname, '../.cache');

// Read manifest.json
let manifest = fs.readFileSync(manifestPath, 'utf8')
  .split('\n')
  .filter(line => line.trim() !== '')
  .map(line => JSON.parse(line));

console.log(`DDB record count: ${Number(manifest.reduce((acc, file) => file.itemCount + acc, 0) / 1e6).toFixed(2)}M records`);

// Function to download a file from S3
async function downloadFile(Key, destPath, fileName) {
  const command = new GetObjectCommand({ Key, Bucket: process.env.LLAMA_COINS_S3_BUCKET });

  try {
    const response = await s3.send(command);

    const totalSize = parseInt(response.ContentLength, 10);
    const fileSizeInMB = (totalSize / (1024 * 1024)).toFixed(2);
    const progressBar = new ProgressBar(`Downloading ${fileName} (${fileSizeInMB} MB) [:bar] :percent :etas`, {
      width: 40,
      complete: '=',
      incomplete: ' ',
      renderThrottle: 100,
      total: totalSize,
    });

    const streamPipeline = promisify(pipeline);

    await streamPipeline(
      response.Body,
      new Transform({
        transform(chunk, encoding, callback) {
          progressBar.tick(chunk.length);
          callback(null, chunk);
        },
      }),
      fs.createWriteStream(destPath)
    );

    console.log('Download complete: ' + fileName);
  } catch (err) {
    console.error('Error', err);
  }
}

// Main function to download all files
async function downloadAllFiles() {
  let overallProgress = new ProgressBar('Overall Progress [:bar] :percent :etas', {
    width: 40,
    total: manifest.length,
  });

  let i = 0;
  for (const file of manifest) {
    i++;
    const fileName = path.basename(file.dataFileS3Key);
    const destPath = path.join(cacheFolderPath, fileName);
    if (!fs.existsSync(destPath)) {
      await downloadFile(file.dataFileS3Key, destPath, fileName +`  (${i}/${manifest.length})`);
    }
    overallProgress.tick();
  }
  console.log('All files downloaded successfully.');
}

// Start the download process
downloadAllFiles().catch((err) => {
  console.error('Error downloading files:', err);
});