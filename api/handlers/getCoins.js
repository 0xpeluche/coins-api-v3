const { getCoinsService } = require("../services/getCoins");

async function getCoinsHandler(request, reply) {
  try {
    const { pids, withMetadata = "false", withTTL = "false" } = request.query;

    if (!pids || typeof pids !== "string") {
      return reply.status(400).send({
        error: "The 'pids' query parameter is required and must be a string."
      });
    }

    const pidArray = pids.split(",").map(pid => pid.trim()).filter(pid => pid);

    if (pidArray.length === 0) {
      return reply.status(400).send({
        error: "The 'pids' query parameter must contain at least one valid pid."
      });
    }

    const includeTTL = withTTL === true || withTTL === "true";
    const includeMetadata = withMetadata === true || withMetadata === "true";

    const result = await getCoinsService(pidArray, {
      withTTL: includeTTL,
      withMetadata: includeMetadata
    });

    return reply.send(result);
  } catch (error) {
    request.log.error("Error in getCoinsHandler:", error);
    return reply.status(500).send({ error: "Internal Server Error" });
  }
}

module.exports = { getCoinsHandler };
