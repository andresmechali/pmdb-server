const config = require("./config.json");
const mysql = require("mysql");
const e = require("express");

const connection = mysql.createPool({
  host: config.rds_host,
  user: config.rds_user,
  password: config.rds_password,
  port: config.rds_port,
  database: config.rds_db,
});

// connection.connect();

connection.on("error", (err) => {
  console.log("error listener:", err);
});

// Christopher
async function home(req, res) {
  const baseQuery = `
  WITH imdb AS (
    SELECT movie_id, rating_score AS imdb_score
    FROM Ratings r
    WHERE num_votes > 0 AND agency_id = 1
    GROUP BY movie_id
  ), tmdb AS (
    SELECT movie_id, rating_score AS tmdb_score
    FROM Ratings r
    WHERE num_votes > 0 AND agency_id = 2
    GROUP BY movie_id
  ), rotten_tomatoes AS (
    SELECT movie_id, rating_score AS rotten_tomatoes_score
   
    FROM Ratings r
    WHERE num_votes > 0 AND agency_id = 3
    GROUP BY movie_id
  )
  SELECT Movies.movie_id, primary_title, start_year, posterPath AS poster_path, imdb_score, tmdb_score, rotten_tomatoes_score
  FROM Movies LEFT OUTER JOIN PMDB.MoviesPosterPath MPP on Movies.movie_id = MPP.tconst
            LEFT OUTER JOIN imdb on imdb.movie_id = Movies.movie_id
            LEFT OUTER JOIN tmdb on tmdb.movie_id = Movies.movie_id
            LEFT OUTER JOIN rotten_tomatoes on rotten_tomatoes.movie_id = Movies.movie_id
  ORDER BY RAND();
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

// Christopher
async function movie_ratings(req, res) {
  //example URL
  //http://127.0.0.1:8080/movie_ratings/1?actor_filter=Leonardo%20DiCaprio&director_filter=Martin%20Scorsese&genre_list_filter=11&genre_list_filter=23&grossing_filter=%3E+128012933
  //Where we look for all movies in which 'Leonardo DiCaprio' playd which are directed by 'Martin Scorsese', which are of the genre 'Biography' OR 'Thriller' and which grossed more than 128012933
  //The result should be the movies 'The Departed' and 'Shutter Island'
  const page = req.params.page || 1;
  const limit = 10;
  const offset = limit * (page - 1);

  const year_filter = req.query.year_filter ? req.query.year_filter : "> 0";
  const IMDB_rating_filter = req.query.IMDB_rating_filter
    ? req.query.IMDB_rating_filter
    : ">= 0";
  const TMDB_rating_filter = req.query.TMDB_rating_filter
    ? req.query.TMDB_rating_filter
    : ">= 0";
  const RT_rating_filter = req.query.RT_rating_filter
    ? req.query.RT_rating_filter
    : ">= 0";
  const genre_list_filter = req.query.genre_list_filter
    ? req.query.genre_list_filter
    : ""; //the list of genres will be an OR filter, the rest is an AND filter
  const actor_filter = req.query.actor_filter ? req.query.actor_filter : "";
  const director_filter = req.query.director_filter
    ? req.query.director_filter
    : "";
  const writer_filter = req.query.writer_filter ? req.query.writer_filter : "";
  const grossing_filter = req.query.grossing_filter
    ? req.query.grossing_filter
    : null;

  const baseQuery = `
  WITH imdb AS (
    SELECT movie_id, rating_score AS imdb_score, num_votes AS imdb_votes
    FROM Ratings r
    WHERE agency_id = 1 
    GROUP BY movie_id
  ), tmdb AS (
    SELECT movie_id, rating_score AS tmdb_score, num_votes AS tmdb_votes
    FROM Ratings r
    WHERE AND agency_id = 2
    GROUP BY movie_id
  ), rotten_tomatoes AS (
    SELECT movie_id, rating_score AS rotten_tomatoes_score, num_votes AS rotten_tomatoes_votes
    FROM Ratings r
    WHERE AND agency_id = 3 
    GROUP BY movie_id
  ), MoviesInclRatings AS (
    SELECT Movies.movie_id AS movie_id, primary_title, original_title, start_year, imdb_score, imdb_votes, tmdb_score, tmdb_votes, rotten_tomatoes_score, rotten_tomatoes_votes, lifetime_grossing
    FROM Movies LEFT OUTER JOIN PMDB.MoviesPosterPath MPP on Movies.movie_id = MPP.tconst
      LEFT JOIN imdb on imdb.movie_id = Movies.movie_id
      LEFT JOIN tmdb on tmdb.movie_id = Movies.movie_id
      LEFT JOIN rotten_tomatoes on rotten_tomatoes.movie_id = Movies.movie_id
    WHERE start_year ${year_filter}
  )
  SELECT DISTINCT M.movie_id AS movie_id, primary_title, start_year, imdb_score, imdb_votes, tmdb_score, tmdb_votes, rotten_tomatoes_score, rotten_tomatoes_votes, lifetime_grossing
  FROM MoviesInclRatings M JOIN HasGenre ON HasGenre.movie_id=M.movie_id
      JOIN Genres G on G.genre_id = HasGenre.genre_id
      JOIN IsCast IC ON M.movie_id = IC.movie_id
      JOIN Persons P_IC on IC.person_id = P_IC.person_id
      JOIN IsDirector ID ON M.movie_id = ID.movie_id
      JOIN Persons P_ID on ID.person_id = P_ID.person_id
      JOIN IsWriter IW ON M.movie_id = IW.movie_id
      JOIN Persons P_IW on IW.person_id = P_IW.person_id
                          
  WHERE ${
    genre_list_filter.length > 0
      ? `(${genre_list_filter
          .map((genre_id) => `G.genre_id = '${genre_id}'`)
          .join(" OR ")}) AND`
      : ""
  } 
    P_IC.primary_name LIKE '%${actor_filter}%' AND P_ID.primary_name LIKE '%${director_filter}%' AND P_IW.primary_name LIKE '%${writer_filter}%' 
    ${
      grossing_filter == null ? "" : ` AND lifetime_grossing ${grossing_filter}`
    } 
    AND rotten_tomatoes_score ${RT_rating_filter} AND tmdb_score ${TMDB_rating_filter} AND imdb_score ${IMDB_rating_filter}
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

  console.log(baseQuery);
}

async function movies(req, res) {
  // a GET request to /movies
  const { value, genres } = req.query;

  const page = req.params.page || 1;
  const limit = 12;
  const offset = limit * (page - 1);
  let genreList = [];
  if (genres) {
    genreList = genres?.split(",");
  }

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
    FROM Movies m
    LEFT JOIN imdb ON m.movie_id = imdb.movie_id
    LEFT JOIN tmdb ON m.movie_id = tmdb.movie_id
    LEFT JOIN rotten_tomatoes ON m.movie_id = rotten_tomatoes.movie_id
    JOIN HasGenre hg ON m.movie_id = hg.movie_id
    ${
      genreList.length > 0
        ? `AND (${genreList
            .map((genre_id) => `hg.genre_id = '${genre_id}'`)
            .join(" OR ")}) `
        : ""
    } 
    ${value ? `WHERE m.primary_title LIKE '%${value}%'` : ""}
    GROUP BY m.movie_id
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

async function all_genres(req, res) {
  const responseHandler = (error, genres) => {
    if (error) {
      console.log(error);
      res.json({ error });
    } else if (genres) {
      res.json({ genres });
    }
  };

  connection.query(
    "SELECT * FROM Genres ORDER BY genre_name ASC;",
    responseHandler
  );
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
  all_genres,
  person,
};
