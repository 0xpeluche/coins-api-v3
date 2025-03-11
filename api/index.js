const { Server } = require('hyper-express');
const fs = require('fs');
const path = require('path');
const coinsRoutes = require('./routes/coinsRoutes');
const { refreshLocalMetadataFromES } = require('./cache/metadataCache');

const node_instance = process.env.NODE_APP_INSTANCE;

if (node_instance == 0) {
  require('./jobs/metadataCron');

  const dataDir = path.join(__dirname, 'data');
  const metadataFilePath = path.join(dataDir, 'metadata.json');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
    console.log("Data directory created:", dataDir);
  }
  if (!fs.existsSync(metadataFilePath)) {
    fs.writeFileSync(metadataFilePath, '{}', 'utf8');
    console.log("Created empty metadata file:", metadataFilePath);
  }
  
  refreshLocalMetadataFromES()
    .then(() => console.log('Metadata cache initialized successfully.'))
    .catch(err => console.error('Error initializing metadata cache:', err));
}

const app = new Server();

app.use(async (req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Api-Key');
  if (req.method === 'OPTIONS') {
    return res.send(200);
  }
  next();
});

app.use('/api/coins', coinsRoutes);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server listening on port http://127.0.0.1:${PORT}/api/coins`);
});
