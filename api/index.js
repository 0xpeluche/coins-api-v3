const fastify = require('fastify')
const { setupSwagger } = require('./plugins/swagger')
const { registerRoutes } = require('./routes')
const { getRedis } = require('../db/redis')

const server = fastify({
  logger: true,
  connectionTimeout: 30000,
  keepAliveTimeout: 5000,
});

server.addHook('onRequest', (request, reply, done) => {
  reply.header('Access-Control-Allow-Origin', '*');
  reply.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
  reply.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, X-Api-Key');

  if (request.method === 'OPTIONS') {
    reply.send();
    return;
  }

  done();
});

setupSwagger(server);
registerRoutes(server);

server.addHook('onClose', (instance, done) => {
  const client = getRedis();
  client.disconnect();
  done();
});

server.setErrorHandler((error, request, reply) => {
  server.log.error(`Error: ${error.message}`);
  reply.status(500).send({ error: "Internal Server Error" });
});

server.listen({ port: 3000, host: '127.0.0.1' }, (err, address) => {
  if (err) {
    server.log.error(err);
    process.exit(1);
  }
  server.log.info(`Server listening at ${address}`);
});
