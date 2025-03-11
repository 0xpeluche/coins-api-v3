module.exports = {
  apps: [
    {
      name: 'coins-api',
      script: 'api/index.js',
      instances: '4',
      exec_mode: 'cluster',
      autorestart: true,
      watch: false,
      env: {
        NODE_ENV: 'production'
      }
    }
  ]
};
