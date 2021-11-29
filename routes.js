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

async function movie_ratings(req, res) {

  //example URL
  //http://127.0.0.1:8080/movie_ratings/1?actor_filter=Leonardo%20DiCaprio&director_filter=Martin%20Scorsese&genre_list_filter=11&genre_list_filter=23&grossing_filter=%3E+128012933
  //Where we look for all movies in which 'Leonardo DiCaprio' playd which are directed by 'Martin Scorsese', which are of the genre 'Biography' OR 'Thriller' and which grossed more than 128012933
  //The result should be the movies 'The Departed' and 'Shutter Island'
  const page = req.params.page || 1;
  const limit = 10;
  const offset = limit * (page - 1);

  const year_filter = req.query.year_filter ? req.query.year_filter : "> 0" ;
  const IMDB_rating_filter = req.query.IMDB_rating_filter ? req.query.IMDB_rating_filter : ">= 0" ;
  const TMDB_rating_filter = req.query.TMDB_rating_filter ? req.query.TMDB_rating_filter : ">= 0" ;
  const RT_rating_filter = req.query.RT_rating_filter ? req.query.RT_rating_filter : ">= 0" ;
  const genre_list_filter = req.query.genre_list_filter ? req.query.genre_list_filter : '' ; //the list of genres will be an OR filter, the rest is an AND filter
  const actor_filter = req.query.actor_filter ? req.query.actor_filter : '';
  const director_filter = req.query.director_filter ? req.query.director_filter : '';
  const writer_filter = req.query.writer_filter ? req.query.writer_filter : '';
  const grossing_filter = req.query.grossing_filter ? req.query.grossing_filter : null ;

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
  WHERE ${genre_list_filter.length > 0 ? `(${genre_list_filter.map((genre_id) => `G.genre_id = '${genre_id}'`).join(" OR ")}) AND` : ''} 
    P_IC.primary_name LIKE '%${actor_filter}%' AND P_ID.primary_name LIKE '%${director_filter}%' AND P_IW.primary_name LIKE '%${writer_filter}%' 
    ${grossing_filter == null ? '' : ` AND lifetime_grossing ${grossing_filter}`} 
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

  console.log(baseQuery)
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

  //example URL
  //http://127.0.0.1:8080/person/nm0000158
  //Where we look for the information of the actor with ID=nm0000158 (which is Tom Hanks)

  const person_id = req.params.person_id;

  const baseQuery = `
  WITH highest_grossing_IsCast AS (
    SELECT DISTINCT P.person_id, M.movie_id AS highest_grossing_IsCast_movie_id, primary_title AS highest_grossing_IsCast_title, lifetime_grossing AS highest_grossing_IsCast_grossing
    FROM Persons P JOIN IsCast IC on P.person_id = IC.person_id
                JOIN Movies M on IC.movie_id = M.movie_id
    WHERE P.person_id = '${person_id}'
    ORDER BY lifetime_grossing DESC#
    LIMIT 1
  ), highest_grossing_IsWriter AS (
      SELECT DISTINCT P.person_id, M.movie_id AS highest_grossing_IsWriter_movie_id, primary_title AS highest_grossing_IsWriter_title, lifetime_grossing AS highest_grossing_IsWriter_grossing
      FROM Persons P JOIN IsWriter IW on P.person_id = IW.person_id
                  JOIN Movies M on IW.movie_id = M.movie_id
      WHERE P.person_id = '${person_id}'
      ORDER BY lifetime_grossing DESC
      LIMIT 1
  ), highest_grossing_IsDirector AS (
      SELECT DISTINCT P.person_id, M.movie_id AS highest_grossing_IsDirector_movie_id, primary_title AS highest_grossing_IsDirector_title, lifetime_grossing AS highest_grossing_IsDirector_grossing
      FROM Persons P JOIN IsDirector ID on P.person_id = ID.person_id
                  JOIN Movies M on ID.movie_id = M.movie_id
      WHERE P.person_id = '${person_id}'
      ORDER BY lifetime_grossing DESC
      LIMIT 1
  ), person_and_highest_grossign_information AS (
      SELECT P.person_id, primary_name, birth_year, death_year,
            highest_grossing_IsCast_movie_id, highest_grossing_IsCast_title, highest_grossing_IsCast_grossing,
            highest_grossing_IsWriter_movie_id, highest_grossing_IsWriter_title, highest_grossing_IsWriter_grossing,
            highest_grossing_IsDirector_movie_id, highest_grossing_IsDirector_title, highest_grossing_IsDirector_grossing
      FROM Persons P JOIN highest_grossing_IsCast IC ON IC.person_id = P.person_id
                      JOIN highest_grossing_IsDirector ID ON ID.person_id = P.person_id
                      JOIN highest_grossing_IsWriter IW ON IW.person_id = P.person_id
  )
  SELECT P.person_id, primary_name, birth_year, death_year,
          highest_grossing_IsCast_movie_id, highest_grossing_IsCast_title, highest_grossing_IsCast_grossing,
          highest_grossing_IsWriter_movie_id, highest_grossing_IsWriter_title, highest_grossing_IsWriter_grossing,
          highest_grossing_IsDirector_movie_id, highest_grossing_IsDirector_title, highest_grossing_IsDirector_grossing,
          M2.movie_id AS movie_id_title_known_for, primary_title AS title_known_for
  FROM person_and_highest_grossign_information P JOIN IsKnownFor IKF ON P.person_id = IKF.person_id
      JOIN Movies M2 on IKF.movie_id = M2.movie_id
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
  home,
  movie_ratings,
  all_genres,
  person
};
