const { getCoinsHandler } = require("../handlers/getCoins");

function getCoinsRoutes(fastify) {
  fastify.get("/coins", {
    schema: {
      description: "Retrieve one or multiple coins.",
      tags: ["Coins"],
      querystring: {
        type: "object",
        properties: {
          pids: {
            type: "string",
            description: "Single pid or multiple pids separated by commas (e.g. 'ethereum:0x123,bitcoin:0x456')"
          },
          withMetadata: {
            type: "boolean",
            description: "Include metadata",
            default: false
          },
          withTTL: {
            type: "boolean",
            description: "Include remaining TTL",
            default: false
          }
        },
        required: ["pids"]
      },
      response: {
        200: {
          description: "Coin data retrieved successfully",
          type: "object",
          properties: {
            coins: {
              type: "array",
              items: {
                type: "object",
                properties: {
                  pid: { type: "string", description: "Coin identifier" },
                  price: { type: "string", description: "Price data" },
                  confidence: { type: "string", description: "Confidence or accuracy of price" },
                  source: { type: "string", description: "Source of the price" },
                  ttl: {
                    type: "number",
                    description: "Time-to-live value (if requested)",
                    nullable: true
                  },
                  metadata: {
                    type: "object",
                    description: "Metadata retrieved (if requested)",
                    nullable: true,
                    additionalProperties: true
                  }
                }
              },
              description: "Array of coin data objects"
            }
          }
        },
        400: {
          description: "Bad Request",
          type: "object",
          properties: {
            error: { type: "string" }
          }
        },
        404: {
          description: "Coins not found",
          type: "object",
          properties: {
            message: {
              type: "string",
              description: "No coins found for the provided IDs"
            }
          }
        },
        500: {
          description: "Internal Server Error",
          type: "object",
          properties: {
            error: { type: "string" }
          }
        }
      }
    },
    handler: getCoinsHandler
  });
}

module.exports = { getCoinsRoutes };
