const { Router } = require('hyper-express');
const coins = require('../controllers/coins');

const router = new Router();

router.get('/metadata', coins.getCoinsMetadata);
router.get('/current', coins.getCoinsCurrent);
router.get('/timeseries', coins.getCoinsTimeseries);
router.get('/earliest', coins.getCoinsFirstTimestamp);
router.get('/percentage-change', coins.getCoinsPercentageChange);

module.exports = router;
