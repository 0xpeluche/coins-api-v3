const { Router } = require("hyper-express");
const router = new Router();
const coinsHandler = require("../handlers/coinsHandler");

router.get("/current", coinsHandler.getCoinsCurrentHandler);
router.get("/metadata", coinsHandler.getCoinsMetadataHandler);
router.get("/timeseries", coinsHandler.getCoinsTimeseriesHandler);

module.exports = router;
