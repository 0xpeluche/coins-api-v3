const swagger = require('@fastify/swagger');
const swaggerUi = require('@fastify/swagger-ui');

async function setupSwagger(fastify) {
  fastify.register(swagger, {
    openapi: {
      info: {
        title: 'API Documentation',
        description: 'Documentation',
        version: '1.0.0'
      }
    }
  });

  fastify.register(swaggerUi, {
    routePrefix: '/documentation',
    exposeRoute: true
  });
}

module.exports = { setupSwagger };
