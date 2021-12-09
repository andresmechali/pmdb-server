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
    /*
        Possible improvement: Filter movies before joining. We can gain about 0.6 seconds.
        
        SELECT RandomMovies.movie_id, primary_title, start_year, posterPath AS poster_path, imdb_score, tmdb_score, rotten_tomatoes_score
        FROM (SELECT * FROM Movies ORDER BY RAND() LIMIT 100) AS RandomMovies
            LEFT OUTER JOIN PMDB.MoviesPosterPath MPP ON RandomMovies.movie_id = MPP.tconst
            LEFT OUTER JOIN imdb on imdb.movie_id = RandomMovies.movie_id
            LEFT OUTER JOIN tmdb on tmdb.movie_id = RandomMovies.movie_id
            LEFT OUTER JOIN rotten_tomatoes on rotten_tomatoes.movie_id = RandomMovies.movie_id
    */
    SELECT Movies.movie_id, primary_title, start_year, posterPath AS poster_path, imdb_score, tmdb_score, rotten_tomatoes_score
    FROM Movies LEFT OUTER JOIN PMDB.MoviesPosterPath MPP on Movies.movie_id = MPP.tconst
      LEFT OUTER JOIN imdb on imdb.movie_id = Movies.movie_id
      LEFT OUTER JOIN tmdb on tmdb.movie_id = Movies.movie_id
      LEFT OUTER JOIN rotten_tomatoes on rotten_tomatoes.movie_id = Movies.movie_id
    ORDER BY RAND()
    LIMIT 100;
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

async function movies(req, res) {
  // a GET request to /movies
  const {
    value,
    genres,
    imdb,
    tmdb,
    rotten_tomatoes,
    min_year,
    max_year,
    budget,
    grossing,
    actors,
    director,
    writer,
  } = req.query;

  const page = req.params.page || 1;
  const limit = 12;
  const offset = limit * (page - 1);
  let genreList = [];
  if (genres) {
    genreList = genres?.split(",");
  }

  let actorsList = [];
  if (actors) {
    actorsList = actors.split(",");
  }

  const baseQuery = `
    WITH imdb AS (
      SELECT movie_id, rating_score AS imdb_score
      FROM Ratings r
      WHERE num_votes > 10 AND agency_id = 1
      ${imdb ? `AND rating_score >= ${imdb}` : ""}
      GROUP BY movie_id
    ), tmdb AS (
      SELECT movie_id, rating_score AS tmdb_score
      FROM Ratings r
      WHERE num_votes > 10 AND agency_id = 2
      ${tmdb ? `AND rating_score >= ${tmdb}` : ""}
      GROUP BY movie_id
    ), rotten_tomatoes AS (
      SELECT movie_id, rating_score AS rotten_tomatoes_score
      FROM Ratings r
      WHERE num_votes > 10 AND agency_id = 3
      ${rotten_tomatoes ? `AND rating_score >= ${rotten_tomatoes}` : ""}
      GROUP BY movie_id
    )
    SELECT
        m.movie_id,
        primary_title,
        start_year,
        runtime_minutes,
        posterPath AS poster_path,
        overview,
        imdb_score,
        tmdb_score,
        rotten_tomatoes_score,
        10 * imdb_score + 10 * tmdb_score + rotten_tomatoes_score - (2022 - m.start_year) / 3 AS total_score,
        budget,
        lifetime_grossing,
        ${
          director
            ? `
              P_ID.person_id AS director_id,
              P_ID.primary_name AS director_name,
            `
            : ""
        }
        ${
          writer
            ? `
              P_IW.person_id AS writer_id,
              P_IW.primary_name AS writer_name,
            `
            : ""
        }
        COUNT(*) OVER() AS full_count
    FROM (
        SELECT * FROM Movies
        LEFT OUTER JOIN PMDB.MoviesPosterPath MPP on Movies.movie_id = MPP.tconst
        WHERE 1
        ${min_year ? `AND start_year >= ${min_year}` : ""}
        ${max_year ? `AND start_year <= ${max_year}` : ""}
        ${budget ? `AND budget >= ${budget}` : ""}
        ${grossing ? `AND lifetime_grossing >= ${grossing}` : ""}
        ${value ? `AND primary_title LIKE '%${value}%'` : ""}
    ) AS m
    ${imdb ? "JOIN" : "LEFT JOIN"} imdb ON m.movie_id = imdb.movie_id
    ${tmdb ? "JOIN" : "LEFT JOIN"} tmdb ON m.movie_id = tmdb.movie_id
    ${
      rotten_tomatoes ? "JOIN" : "LEFT JOIN"
    } rotten_tomatoes ON m.movie_id = rotten_tomatoes.movie_id
    JOIN HasGenre hg ON m.movie_id = hg.movie_id
    ${
      genreList.length > 0
        ? `AND (${genreList
            .map((genre_id) => `hg.genre_id = '${genre_id}'`)
            .join(" OR ")}) `
        : ""
    }
    JOIN IsCast IC ON m.movie_id = IC.movie_id
    JOIN Persons P_IC ON IC.person_id = P_IC.person_id
    ${
      actorsList.length > 0
        ? `AND (${actorsList
            .map((name) => `P_IC.primary_name LIKE '%${name}%'`)
            .join(" OR ")})`
        : ""
    }
    
    ${
      director
        ? `
          JOIN IsDirector ID ON m.movie_id = ID.movie_id
          JOIN Persons P_ID on ID.person_id = P_ID.person_id AND P_ID.primary_name LIKE '%${director}%'
        `
        : ""
    }
    ${
      writer
        ? `
          JOIN IsWriter IW ON m.movie_id = IW.movie_id
          JOIN Persons P_IW on IW.person_id = P_IW.person_id AND P_IW.primary_name LIKE '%${writer}%'
        `
        : ""
    }
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

// Manuel
async function movie(req, res) {
  const movie_id = req.params.movie_id;

  const baseQuery = `
    WITH imdb_ratings AS (
       SELECT movie_id, rating_score AS imdb_score, num_votes AS imdb_votes
       FROM Ratings r
       WHERE agency_id = 1
       GROUP BY movie_id
    ), tmdb_ratings AS (
       SELECT movie_id, rating_score AS tmdb_score, num_votes AS tmdb_votes
       FROM Ratings r
       WHERE agency_id = 2
       GROUP BY movie_id
    ), rt_ratings AS (
       SELECT movie_id, rating_score AS rt_score, num_votes AS rt_votes
       FROM Ratings r
       WHERE agency_id = 3
       GROUP BY movie_id
    )
    SELECT
       m.primary_title,
       m.original_title,
       m.start_year AS year,
       m.budget ,
       m.runtime_minutes,
       m.lifetime_grossing,
       m.overview AS summary,
       m.homepage,
       m.is_adult AS rated_r,
       i.imdb_score,
       t.tmdb_score,
       r.rt_score,
       GROUP_CONCAT(DISTINCT g.genre_name) AS genres
    FROM (SELECT * FROM Movies WHERE movie_id = '${movie_id}') m
    LEFT JOIN imdb_ratings i ON m.movie_id = i.movie_id
    LEFT JOIN tmdb_ratings t ON m.movie_id = t.movie_id
    LEFT JOIN rt_ratings r ON m.movie_id = r.movie_id
    LEFT JOIN HasGenre hg ON hg.movie_id = m.movie_id
    LEFT JOIN Genres g on g.genre_id = hg.genre_id
  `;

  const responseHandler = (error, results) => {
    if (error || results?.length === 0) {
      console.log(error);
      res.json({ error });
    } else if (results) {
      res.json({ movie: results[0] });
    }
  };

  connection.query(baseQuery, responseHandler);
}

// Manuel
async function movie_rec(req, res) {
  const movie_id = req.params.movie_id;

  const baseQuery = `
  WITH imdb_ratings AS (
  SELECT movie_id, rating_score AS imdb_score, num_votes AS imdb_votes
  FROM Ratings r
  WHERE agency_id = 1
  GROUP BY movie_id
  ),
  tmdb_ratings AS (
  SELECT movie_id, rating_score AS tmdb_score, num_votes AS tmdb_votes
  FROM Ratings r
  WHERE agency_id = 2
  GROUP BY movie_id
  ),
  rt_ratings AS (
  SELECT movie_id, rating_score AS rt_score, num_votes AS rt_votes
  FROM Ratings r
  WHERE agency_id = 3
  GROUP BY movie_id
  ),
  movie_genre AS (
  SELECT m.movie_id,
  GROUP_CONCAT(DISTINCT g.genre_name) AS genres
  FROM (SELECT * FROM Movies WHERE movie_id = '${movie_id}') m
  LEFT JOIN HasGenre hg ON hg.movie_id = m.movie_id
  LEFT JOIN Genres g on g.genre_id = hg.genre_id),
  all_movie_genres AS (
  SELECT m.movie_id,
  GROUP_CONCAT(DISTINCT g.genre_name) AS genres
  FROM Movies m
  LEFT JOIN HasGenre hg ON hg.movie_id = m.movie_id
  LEFT JOIN Genres g on g.genre_id = hg.genre_id
  GROUP BY 1)
  SELECT m.primary_title AS movie_title,
  a.genres AS genres,
  ((i.imdb_score + t.tmdb_score + (r.rt_score / 10)) / 3) AS average_score
  FROM (SELECT * FROM Movies WHERE movie_id != '${movie_id}') m
  LEFT JOIN imdb_ratings i ON m.movie_id = i.movie_id
  LEFT JOIN tmdb_ratings t ON m.movie_id = t.movie_id
  LEFT JOIN rt_ratings r ON m.movie_id = r.movie_id
  LEFT JOIN all_movie_genres a ON m.movie_id = a.movie_id
  WHERE genres LIKE (SELECT genres FROM movie_genre)
  ORDER BY average_score DESC
  LIMIT 10;
  `;

  const responseHandler = (error, results) => {
    if (error || results?.length === 0) {
      console.log(error);
      res.json({ error });
    } else if (results) {
      res.json({ movie: results});
    }
  };

  connection.query(baseQuery, responseHandler);
}

async function movie_person_rec(req, res) {
  const person_id = req.params.person_id;

  const baseQuery = `
  WITH cast_filter AS (
    SELECT DISTINCT(m.primary_title) AS movies,
    c.movie_id
    FROM Persons p
    LEFT JOIN IsCast c ON p.person_id = c.person_id
    LEFT JOIN Movies m ON c.movie_id = m.movie_id
    WHERE p.person_id LIKE '${person_id}'
  ), directed_filter AS (
    SELECT DISTINCT(m.primary_title) AS movies,
    c.movie_id
    FROM Persons p
    LEFT JOIN IsDirector c ON p.person_id = c.person_id
    LEFT JOIN Movies m ON c.movie_id = m.movie_id
    WHERE p.person_id LIKE '${person_id}'
  ), written_filter AS (
    SELECT DISTINCT(m.primary_title) AS movies,
    c.movie_id
    FROM Persons p
    LEFT JOIN IsWriter c ON p.person_id = c.person_id
    LEFT JOIN Movies m ON c.movie_id = m.movie_id
    WHERE p.person_id LIKE '${person_id}'
  ), all_movies AS (
    SELECT * FROM cast_filter WHERE movies IS NOT NULL
    UNION
    SELECT * FROM directed_filter WHERE movies IS NOT NULL
    UNION
    SELECT * FROM written_filter WHERE movies IS NOT NULL
 ), imdb_ratings AS (
    SELECT movie_id, rating_score AS imdb_score, num_votes AS imdb_votes
    FROM Ratings r
    WHERE agency_id = 1
    GROUP BY movie_id
  ), tmdb_ratings AS (
    SELECT movie_id, rating_score AS tmdb_score, num_votes AS tmdb_votes
    FROM Ratings r
    WHERE agency_id = 2
    GROUP BY movie_id
  ), rt_ratings AS (
    SELECT movie_id, rating_score AS rt_score, num_votes AS rt_votes
    FROM Ratings r
    WHERE agency_id = 3
    GROUP BY movie_id
  )
  SELECT m.movies AS movie_title,
  ((i.imdb_score + t.tmdb_score + (r.rt_score / 10)) / 3) AS average_score
  FROM all_movies m
  LEFT JOIN imdb_ratings i ON m.movie_id = i.movie_id
  LEFT JOIN tmdb_ratings t ON m.movie_id = t.movie_id
  LEFT JOIN rt_ratings r ON m.movie_id = r.movie_id
  ORDER BY average_score DESC
  LIMIT 10;
  `;

  const responseHandler = (error, results) => {
    if (error || results?.length === 0) {
      console.log(error);
      res.json({ error });
    } else if (results) {
      res.json({ person: results});
    }
  };

  connection.query(baseQuery, responseHandler);
}

async function find_actors(req, res) {
  const page = req.params.page || 1;
  const limit = 10;
  const offset = limit * (page - 1);

  const birth_year = req.query.birth_year ? req.query.birth_year : "> 0";
  const death_year = req.query.death_year ? req.query.death_year : "> 0";
  const movie_acted = req.query.movie_acted ? req.query.movie_acted : "";
  const movie_directed = req.query.movie_directed
    ? req.query.movie_directed
    : "";
  const movie_written = req.query.movie_written ? req.query.movie_written : "";

  const baseQuery = `
  WITH cast_filter AS (
    SELECT DISTINCT(p.person_id) AS actors
    FROM Persons p
    LEFT JOIN IsCast c ON p.person_id = c.person_id
    LEFT JOIN Movies m ON c.movie_id = m.movie_id
    WHERE m.primary_title LIKE '%${movie_acted}%'
  ), directed_filter AS (
    SELECT DISTINCT(p.person_id) AS directors
    FROM Persons p
    LEFT JOIN IsDirector c ON p.person_id = c.person_id
    LEFT JOIN Movies m ON c.movie_id = m.movie_id
    WHERE m.primary_title LIKE '%${movie_directed}%'
  ), written_filter AS (
    SELECT DISTINCT(p.person_id) AS writers
    FROM Persons p
    LEFT JOIN IsWriter c ON p.person_id = c.person_id
    LEFT JOIN Movies m ON c.movie_id = m.movie_id
    WHERE m.primary_title LIKE '%${movie_written}%'
  ), movies_known_for AS (
    SELECT i.person_id, m.primary_title
    FROM IsKnownFor i
    LEFT JOIN Movies m
        ON i.movie_id = m.movie_id
  )
  SELECT p.primary_name, p.birth_year, p.death_year,
     GROUP_CONCAT(DISTINCT i.primary_title)  AS movies_known_for,
     COUNT(c.person_id)  AS movies_acted,
     COUNT(d.person_id) AS movies_directed,
     COUNT(w.person_id)  AS movies_written
  FROM (SELECT * FROM Persons WHERE person_id IN ((SELECT actors FROM cast_filter) UNION (SELECT directors FROM directed_filter) UNION (SELECT writers FROM written_filter))) p
  LEFT JOIN movies_known_for i
      ON p.person_id = i.person_id
  LEFT JOIN IsCast c
      ON p.person_id = c.person_id
  LEFT JOIN IsDirector d
      ON p.person_id = d.person_id
  LEFT JOIN IsWriter w
      ON p.person_id = w.person_id
  GROUP BY p.primary_name, p.birth_year, p.death_year
  HAVING ${
    birth_year == null
      ? "p.birth_year IS NOT NULL OR p.birth_year IS NULL"
      : `p.birth_year ${birth_year}`
  } 
    ${death_year == null ? "" : `AND p.death_year ${death_year}`}
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

/*
 * All the actors for a given movie_id
 * */
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

/*
 * Director and writer for a given movie_id
 * */
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

/*
 * List of all the genres to be used on the genre filter
 * This could be hardcoded on the front-end to avoid querying
 * */
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

/*
 * Get data for a given person_id
 * */
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
  home,
  movies,
  movie_rec,
  movie_person_rec,
  movie,
  movie_cast,
  movie_director_and_writer,
  all_genres,
  person,
};
