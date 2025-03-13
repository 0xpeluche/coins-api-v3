const { Router } = require('hyper-express');
const coinsController = require('../controllers/coinsController');

const router = new Router();

router.get('/metadata', coinsController.getCoinsMetadata);
router.get('/current', coinsController.getCoinsCurrent);
router.get('/timeseries', coinsController.getCoinsTimeseries);
router.get('/earliest', coinsController.getCoinFirstTimestamp);
router.get('/percentage-change', coinsController.getPercentageChange);

module.exports = router;
