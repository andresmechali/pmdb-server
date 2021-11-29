const express = require("express");
const mysql = require("mysql");
const cors = require("cors");

const routes = require("./routes");
const config = require("./config.json");

const app = express();

app.use(
  cors({
    origin: "*",
  })
);

// Get actors/directors/writters information
app.get("/find_actors/:page", routes.find_actors);

app.get("/movie/:movie_id", routes.movie);

app.listen(config.server_port, () => {
  console.log(
    `Server running at http://${config.server_host}:${config.server_port}/`
  );
});

module.exports = app;
