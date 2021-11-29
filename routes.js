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

async function movie(req, res) {
  const movie_id = req.params.movie_id;

  const baseQuery = `
  WITH imdb_ratings AS (
    SELECT movie_id, rating_score AS imdb_scores, num_votes AS imdb_votes
    FROM Ratings r
    WHERE agency_id = 1
    GROUP BY movie_id
  ), tmdb_ratings AS (
    SELECT movie_id, rating_score AS tmdb_scores, num_votes AS tmdb_votes
    FROM Ratings r
    WHERE agency_id = 2
    GROUP BY movie_id
  ), rt_ratings AS (
    SELECT movie_id, rating_score AS rt_scores, num_votes AS rt_votes
    FROM Ratings r
    WHERE agency_id = 3
    GROUP BY movie_id
  ), movie_directors AS (
    SELECT movie_id, GROUP_CONCAT(DISTINCT primary_name)  AS directors
    FROM Persons p
    LEFT JOIN IsDirector d ON p.person_id = d.person_id
    GROUP BY movie_id
    HAVING movie_id = '${movie_id}'
  ), movie_writers AS (
    SELECT movie_id, GROUP_CONCAT(DISTINCT primary_name) AS writers
    FROM Persons p
    LEFT JOIN IsWriter d ON p.person_id = d.person_id
    GROUP BY movie_id
    HAVING movie_id = '${movie_id}'
  ), movie_actors AS (
    SELECT movie_id, GROUP_CONCAT(DISTINCT primary_name) AS actors
    FROM Persons p
    LEFT JOIN IsCast d ON p.person_id = d.person_id
    GROUP BY movie_id
    HAVING movie_id = '${movie_id}'
  )
  SELECT m.primary_title,
   m.original_title,
   m.start_year AS year,
   m.budget ,
   m.runtime_minutes,
   m.lifetime_grossing,
   m.overview AS summary,
   m.homepage,
   m.is_adult AS rated_r,
   i.imdb_scores,
   t.tmdb_scores,
   r.rt_scores,
   md.directors,
   mw.writers,
   ma.actors
  FROM (SELECT * FROM Movies WHERE movie_id = '${movie_id}') m
  LEFT JOIN imdb_ratings i ON m.movie_id = i.movie_id
  LEFT JOIN tmdb_ratings t ON m.movie_id = t.movie_id
  LEFT JOIN rt_ratings r ON m.movie_id = r.movie_id
  LEFT JOIN movie_directors md ON m.movie_id = md.movie_id
  LEFT JOIN movie_writers mw ON m.movie_id = mw.movie_id
  LEFT JOIN movie_actors ma ON m.movie_id = ma.movie_id;
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

  console.log(baseQuery)
}

async function find_actors(req, res) {
  const page = req.params.page || 1;
  const limit = 10;
  const offset = limit * (page - 1);

  const birth_year = req.query.birth_year ? req.query.birth_year : "> 0";
  const death_year = req.query.death_year ? req.query.death_year : "> 0" ;
  const movie_acted = req.query.movie_acted ? req.query.movie_acted : '';
  const movie_directed = req.query.movie_directed ? req.query.movie_directed : '' ;
  const movie_written = req.query.movie_written ? req.query.movie_written : '' ;

  const baseQuery = `
  WITH cast_filter AS (
    SELECT DISTINCT(p.person_id) AS actors
    FROM Persons p
    LEFT JOIN IsCast c ON p.person_id = c.person_id
    LEFT JOIN Movies m ON c.movie_id = m.movie_id
    WHERE m.primary_title LIKE '${movie_acted}'
  ), directed_filter AS (
    SELECT DISTINCT(p.person_id) AS directors
    FROM Persons p
    LEFT JOIN IsDirector c ON p.person_id = c.person_id
    LEFT JOIN Movies m ON c.movie_id = m.movie_id
    WHERE m.primary_title LIKE '${movie_directed}'
  ), written_filter AS (
    SELECT DISTINCT(p.person_id) AS writers
    FROM Persons p
    LEFT JOIN IsWriter c ON p.person_id = c.person_id
    LEFT JOIN Movies m ON c.movie_id = m.movie_id
    WHERE m.primary_title LIKE '${movie_written}'
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
  HAVING p.birth_year ${birth_year} 
    AND p.death_year ${death_year}
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

  console.log(baseQuery)
}

module.exports = {
  movie,
  find_actors
};
