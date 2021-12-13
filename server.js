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

// Get movies for a given page
app.get("/home", routes.home);

// Get movies for a given page
app.get("/movies/:page", routes.movies);

// Get basic information about a movie
app.get("/movie/:movie_id", routes.movie);

// Get cast of a given movie
app.get("/movie-cast/:movie_id", routes.movie_cast);

// Get director and writer for a given movie
app.get(
  "/movie-director-and-writer/:movie_id",
  routes.movie_director_and_writer
);

// Get all genres
app.get("/genres", routes.all_genres);

// Get persons for a given page
app.get("/persons/:page", routes.persons);

// Get basic information about a person
app.get("/person/:person_id", routes.person);

// Get movie recommendations based on a given movie
app.get("/recommendations/movie/:movie_id", routes.movie_rec);

// Get movie recommendations based on a given person
app.get("/recommendations/person/:person_id", routes.movie_person_rec);

app.listen(config.server_port, () => {
  console.log(
    `Server running at http://${config.server_host}:${config.server_port}/`
  );
});

module.exports = app;
