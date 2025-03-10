const cron = require('node-cron');
const { refreshLocalMetadataFromES } = require('../services/coinsService');

cron.schedule('*/15 * * * *', () => {
  refreshLocalMetadataFromES()
    .then(() => {
      console.log('Metadata cache refreshed successfully.');
    })
    .catch((err) => {
      console.error('Error refreshing metadata cache:', err);
    });
});
