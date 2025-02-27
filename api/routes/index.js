const { getCoinsRoutes } = require('./getCoins')

async function registerRoutes(fastify) {
  await fastify.register(getCoinsRoutes);
}

module.exports = { registerRoutes }
