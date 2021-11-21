const config = require("./config.json");
const mysql = require("mysql");
const e = require("express");

// TODO: fill in your connection details here
const connection = mysql.createConnection({
  host: config.rds_host,
  user: config.rds_user,
  password: config.rds_password,
  port: config.rds_port,
  database: config.rds_db,
});

connection.connect();

async function movies(req, res) {
  // a GET request to /movies
  const page = req.params.page || 1;
  const limit = 12;
  const offset = limit * (page - 1);
  const baseQuery = `
    WITH imdb AS (
      SELECT movie_id, rating_score AS imdb_score
      FROM Ratings r
      WHERE num_votes > 100 AND agency_id = 1
      GROUP BY movie_id
    ), tmdb AS (
      SELECT movie_id, rating_score AS tmdb_score
      FROM Ratings r
      WHERE num_votes > 100 AND agency_id = 2
      GROUP BY movie_id
    ), rotten_tomatoes AS (
      SELECT movie_id, rating_score AS rotten_tomatoes_score
      FROM Ratings r
      WHERE num_votes > 10 AND agency_id = 3
      GROUP BY movie_id
    )
    SELECT m.movie_id, primary_title, start_year, runtime_minutes, poster_path, overview, imdb_score, tmdb_score, rotten_tomatoes_score, 10 * imdb_score + 10 * tmdb_score + rotten_tomatoes_score - (2022 - m.start_year) / 3 AS total_score, COUNT(*) OVER() AS full_count
    FROM Movies m JOIN imdb JOIN tmdb JOIN rotten_tomatoes ON m.movie_id = imdb.movie_id AND m.movie_id = tmdb.movie_id AND m.movie_id = rotten_tomatoes.movie_id
    ORDER BY total_score DESC
    LIMIT ${limit}
    OFFSET ${offset};
  `;

  const responseHandler = (error, results) => {
    if (error) {
      console.log(error);
      res.json({ error });
    } else if (results) {
      res.json({ results });
    }
  };

  connection.query(baseQuery, responseHandler);
}

async function movie(req, res) {
  const movie_id = req.params.movie_id;

  const baseQuery = `
    WITH imdb AS (
      SELECT movie_id, rating_score AS imdb_score
      FROM Ratings r
      WHERE num_votes > 100 AND agency_id = 1
      GROUP BY movie_id
    ), tmdb AS (
      SELECT movie_id, rating_score AS tmdb_score
      FROM Ratings r
      WHERE num_votes > 100 AND agency_id = 2
      GROUP BY movie_id
    ), rotten_tomatoes AS (
      SELECT movie_id, rating_score AS rotten_tomatoes_score
      FROM Ratings r
      WHERE num_votes > 10 AND agency_id = 3
      GROUP BY movie_id
    )
    SELECT m.movie_id, primary_title, start_year, runtime_minutes, poster_path, overview, imdb_score, tmdb_score, rotten_tomatoes_score, 10 * imdb_score + 10 * tmdb_score + rotten_tomatoes_score AS total_score
    FROM Movies m JOIN imdb JOIN tmdb JOIN rotten_tomatoes ON m.movie_id = "${movie_id}" AND m.movie_id = imdb.movie_id AND m.movie_id = tmdb.movie_id AND m.movie_id = rotten_tomatoes.movie_id
    LIMIT 1
  `;

  const responseHandler = (error, movie) => {
    if (error) {
      console.log(error);
      res.json({ error });
    } else if (movie && movie[0]) {
      res.json({ movie: movie[0] });
    }
  };

  connection.query(baseQuery, responseHandler);
}

async function movie_cast(req, res) {
  const movie_id = req.params.movie_id;

  const baseQuery = `
    SELECT p.person_id, primary_name, birth_year, death_year, job_category, characters
    FROM Persons p JOIN IsCast ic ON p.person_id = ic.person_id
    WHERE ic.movie_id = '${movie_id}'
  `;

  const responseHandler = (error, cast) => {
    if (error) {
      console.log(error);
      res.json({ error });
    } else if (cast) {
      res.json({ cast });
    }
  };

  if (movie_id) {
    connection.query(baseQuery, responseHandler);
  } else {
    res.json({ error: "movie_id not provided" });
  }
}

async function movie_director_and_writer(req, res) {
  const movie_id = req.params.movie_id;

  const baseQuery = `
    WITH Director AS (
        SELECT DISTINCT p.person_id, primary_name, TRUE AS is_director
    FROM Persons p JOIN IsDirector id on p.person_id = id.person_id
    WHERE id.movie_id = '${movie_id}'
    ), Writer AS (
        SELECT DISTINCT p.person_id, primary_name, TRUE as is_writer
    FROM Persons p JOIN IsWriter iw on p.person_id = iw.person_id
    WHERE iw.movie_id = '${movie_id}'
    )
    SELECT d.person_id, d.primary_name, is_director, is_writer
    FROM Director d JOIN Writer w ON d.person_id = w.person_id;
  `;

  const responseHandler = (error, directorAndWriter) => {
    if (error) {
      console.log(error);
      res.json({ error });
    } else if (directorAndWriter) {
      res.json({ directorAndWriter });
    }
  };

  if (movie_id) {
    connection.query(baseQuery, responseHandler);
  } else {
    res.json({ error: "movie_id not provided" });
  }
}

async function movie_genres(req, res) {
  const movie_id = req.params.movie_id;

  const baseQuery = `
    SELECT genre_name
    FROM Genres g JOIN HasGenre hg on g.genre_id = hg.genre_id
    WHERE hg.movie_id = '${movie_id}'
  `;

  const responseHandler = (error, genres) => {
    if (error) {
      console.log(error);
      res.json({ error });
    } else if (genres) {
      res.json({ genres });
    }
  };

  if (movie_id) {
    connection.query(baseQuery, responseHandler);
  } else {
    res.json({ error: "movie_id not provided" });
  }
}

async function person(req, res) {
  const person_id = req.params.person_id;

  const baseQuery = `
    SELECT p.person_id, primary_name, birth_year, death_year, m.movie_id, primary_title, start_year, IC.characters
    FROM Persons p
    LEFT JOIN IsKnownFor IKF ON p.person_id = IKF.person_id
    LEFT JOIN Movies m ON IKF.movie_id = m.movie_id
    LEFT JOIN IsCast IC on m.movie_id = IC.movie_id AND p.person_id = IC.person_id
    WHERE p.person_id = '${person_id}';
  `;

  const responseHandler = (error, person) => {
    if (error) {
      console.log(error);
      res.json({ error });
    } else if (person) {
      res.json({ person });
    }
  };

  if (person_id) {
    connection.query(baseQuery, responseHandler);
  } else {
    res.json({ error: "person_id not provided" });
  }
}

module.exports = {
  movies,
  movie,
  movie_cast,
  movie_director_and_writer,
  movie_genres,
  person,
};
