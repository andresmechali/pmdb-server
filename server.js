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


//get movie information for the homepage
app.get("/home", routes.home);

//get movie ratings list based on filtering
app.get("/movie_ratings/:page", routes.movie_ratings)

// Get all genres
app.get("/genres", routes.all_genres);

app.listen(config.server_port, () => {
  console.log(
    `Server running at http://${config.server_host}:${config.server_port}/`
  );
});

module.exports = app;
