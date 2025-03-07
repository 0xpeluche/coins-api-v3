const coinsController = require('../controllers/coinsController');

async function getCoinsCurrentHandler(req, res) {
  try {
    const data = await coinsController.getCoinsCurrentController(req.query);
    res.send(JSON.stringify(data));
  } catch (error) {
    res.statusCode = 400;
    res.send(JSON.stringify({ error: error.message || "Internal Server Error" }));
  }
}

async function getCoinsMetadataHandler(req, res) {
  try {
    const data = await coinsController.getCoinsMetadataController(req.query);
    res.send(JSON.stringify(data));
  } catch (error) {
    res.statusCode = 400;
    res.send(JSON.stringify({ error: error.message || "Internal Server Error" }));
  }
}

async function getCoinsTimeseriesHandler(req, res) {
  try {
    const data = await coinsController.getCoinsTimeseriesController(req.query);
    res.send(JSON.stringify(data));
  } catch (error) {
    res.statusCode = 400;
    res.send(JSON.stringify({ error: error.message || "Internal Server Error" }));
  }
}

module.exports = {
  getCoinsCurrentHandler,
  getCoinsMetadataHandler,
  getCoinsTimeseriesHandler,
};
