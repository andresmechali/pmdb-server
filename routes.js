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


async function Person(req, res) {

  const person_id = req.params.person_id;

  const baseQuery = `
  WITH highest_grossing_IsCast AS (
    SELECT DISTINCT P.person_id, M.movie_id AS highest_grossing_IsCast_movie_id, primary_title AS highest_grossing_IsCast_title, lifetime_grossing AS highest_grossing_IsCast_grossing
    FROM Persons P JOIN IsCast IC on P.person_id = IC.person_id
                JOIN Movies M on IC.movie_id = M.movie_id
    WHERE P.person_id = ${person_id}
    ORDER BY lifetime_grossing DESC#
    LIMIT 1
), highest_grossing_IsWriter AS (
  SELECT DISTINCT P.person_id, M.movie_id AS highest_grossing_IsWriter_movie_id, primary_title AS highest_grossing_IsWriter_title, lifetime_grossing AS highest_grossing_IsWriter_grossing
  FROM Persons P JOIN IsWriter IW on P.person_id = IW.person_id
              JOIN Movies M on IW.movie_id = M.movie_id
  WHERE P.person_id = ${person_id}
  ORDER BY lifetime_grossing DESC
  LIMIT 1
), highest_grossing_IsDirector AS (
  SELECT DISTINCT P.person_id, M.movie_id AS highest_grossing_IsDirector_movie_id, primary_title AS highest_grossing_IsDirector_title, lifetime_grossing AS highest_grossing_IsDirector_grossing
  FROM Persons P JOIN IsDirector ID on P.person_id = ID.person_id
              JOIN Movies M on ID.movie_id = M.movie_id
  WHERE P.person_id = ${person_id}
  ORDER BY lifetime_grossing DESC
  LIMIT 1
), person_and_highest_grossign_information AS (
  SELECT P.person_id, primary_name, birth_year, death_year,
        highest_grossing_IsCast_movie_id, highest_grossing_IsCast_title, highest_grossing_IsCast_grossing,
        highest_grossing_IsWriter_movie_id, highest_grossing_IsWriter_title, highest_grossing_IsWriter_grossing,
        highest_grossing_IsDirector_movie_id, highest_grossing_IsDirector_title, highest_grossing_IsDirector_grossing
  FROM Persons P LEFT JOIN highest_grossing_IsCast IC ON IC.person_id = P.person_id
                  LEFT JOIN highest_grossing_IsDirector ID ON ID.person_id = P.person_id
                  LEFT JOIN highest_grossing_IsWriter IW ON IW.person_id = P.person_id
  WHERE P.person_id = ${person_id}
)
SELECT P.person_id, primary_name, birth_year, death_year,
      highest_grossing_IsCast_movie_id, highest_grossing_IsCast_title, highest_grossing_IsCast_grossing,
      highest_grossing_IsWriter_movie_id, highest_grossing_IsWriter_title, highest_grossing_IsWriter_grossing,
      highest_grossing_IsDirector_movie_id, highest_grossing_IsDirector_title, highest_grossing_IsDirector_grossing,
      GROUP_CONCAT(M2.movie_id) AS movie_id_title_known_for, GROUP_CONCAT( distinct primary_title) AS title_known_for
FROM person_and_highest_grossign_information P JOIN IsKnownFor IKF ON P.person_id = IKF.person_id
  JOIN Movies M2 on IKF.movie_id = M2.movie_id;
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

async function actors_work_with(req, res) {

  const page = req.params.page || 1;
  const limit = 10;
  const offset = limit * (page - 1);

  const name_filter = req.query.name_filter ? req.query.name_filter : '';


  const baseQuery = `
  
  SELECT distinct P.person_id , P.primary_name # THIS QUERY JUST THE PERSON THAT PLAYED WITH TOM HARDY
  FROM Persons JOIN IsCast IC on Persons.person_id = IC.person_id
      JOIN Movies M on M.movie_id = IC.movie_id
      JOIN IsCast I on M.movie_id = I.movie_id
      JOIN Persons P on P.person_id = I.person_id
  WHERE Persons.primary_name = ${name_filter};`

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

async function actors_worked_with_count_acted(req, res) {

  const page = req.params.page || 1;
  const limit = 10;
  const offset = limit * (page - 1);

  const person_id = req.query.name_filter ? req.query.name_filter : '';


  const baseQuery = `
  
  SELECT p.person_id,p.primary_name ,p.Born, p.Death, p.movie_id_title_known_for,p.List_movie_knownfor,COUNT(*) as number_movie_acted_in
  FROM Persons JOIN IsCast IC on Persons.person_id = IC.person_id
      JOIN Movies M on M.movie_id = IC.movie_id
      Join p6 p on Persons.person_id = p.person_id
  WHERE Persons.person_id  = ${person_id}; #name of paul fox example `

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

async function aactors_worked_with_count_directed(req, res) {

  const page = req.params.page || 1;
  const limit = 10;
  const offset = limit * (page - 1);

  const person_id = req.query.name_filter ? req.query.name_filter : '';


  const baseQuery = `
  
  SELECT p.person_id,p.primary_name ,p.Born, p.Death,p.movie_id_title_known_for, p.List_movie_knownfor,COUNT(*) as number_movie_directed_in
  FROM Persons JOIN IsDirector ID on Persons.person_id = ID.person_id
      JOIN Movies M on M.movie_id = ID.movie_id
      Join p6 p on Persons.person_id = p.person_id
  WHERE Persons.person_id  = ${person_id}; #name of paul fox example `

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

async function actors_worked_with_count_written(req, res) {

  const page = req.params.page || 1;
  const limit = 10;
  const offset = limit * (page - 1);

  const person_id = req.query.name_filter ? req.query.name_filter : '';


  const baseQuery = `
  
  SELECT p.person_id,p.primary_name ,p.Born, p.Death,p.movie_id_title_known_for, p.List_movie_knownfor,COUNT(*) as number_movie_written
  FROM Persons JOIN IsWriter IW on Persons.person_id = IW.person_id
      JOIN Movies M on M.movie_id = IW.movie_id
      Join p6 p on Persons.person_id = p.person_id
  WHERE Persons.person_id  = ${person_id}; #name of paul fox example `

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
};