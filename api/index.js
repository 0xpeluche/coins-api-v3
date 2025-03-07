const { Server } = require("hyper-express");
const coinsRoutes = require("./routes");
const app = new Server();

app.use(async (req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET,PUT,POST,DELETE,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Api-Key");
  if (req.method === "OPTIONS") {
    return res.send(200);
  }
  next();
});

app.use("/api/coins", coinsRoutes);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server listening on port http://127.0.0.1:${PORT}/api/coins`);
});
