module.exports = {
  apps: [
    {
      name: "openboy-engine",
      script: "./openboy-engine",
      args: "-port 4040",
      cwd: "./",
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: "100M",
      env: {
        SERVER_PORT: "4040",
      },
    },
  ],
};
