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
    SELECT RandomMovies.movie_id, primary_title, start_year, poster_path, imdb_score, tmdb_score, rotten_tomatoes_score
    FROM (SELECT * FROM Movies ORDER BY RAND() LIMIT 100) AS RandomMovies
      LEFT OUTER JOIN imdb on imdb.movie_id = RandomMovies.movie_id
      LEFT OUTER JOIN tmdb on tmdb.movie_id = RandomMovies.movie_id
      LEFT OUTER JOIN rotten_tomatoes on rotten_tomatoes.movie_id = RandomMovies.movie_id
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
    name,
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
        poster_path,
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
        WHERE 1
        ${min_year ? `AND start_year >= ${min_year}` : ""}
        ${max_year ? `AND start_year <= ${max_year}` : ""}
        ${budget ? `AND budget >= ${budget}` : ""}
        ${grossing ? `AND lifetime_grossing >= ${grossing}` : ""}
        ${name ? `AND primary_title LIKE '%${name}%'` : ""}
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
              .map((actor_name) => `P_IC.primary_name LIKE '%${actor_name}%'`)
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
async function persons(req, res) {
  const {
    name,
    min_birth_year,
    max_birth_year,
    min_death_year,
    max_death_year,
    movie_name,
    roles,
  } = req.query;

  const page = req.params.page || 1;
  const limit = 12;
  const offset = limit * (page - 1);

  const selectedRoles = roles?.split(",");
  let isCast = selectedRoles?.indexOf("cast") > -1;
  let isDirector = selectedRoles?.indexOf("director") > -1;
  let isWriter = selectedRoles?.indexOf("writer") > -1;

  let hasRole = isCast || isDirector || isWriter;
  const hasAllRoles = isCast && isDirector && isWriter;

  if (!hasRole) {
    isCast = true;
    isDirector = true;
    isWriter = true;
  }

  if (hasAllRoles) {
    hasRole = false;
  }

  const baseQuery = `
    WITH participated AS (
        ${[
    `
            SELECT person_id, movie_id, 'Cast' AS role
            FROM IsCast    
          `,
    `
            SELECT person_id, movie_id, 'Director' AS role
            FROM IsDirector
          `,
    `
            SELECT person_id, movie_id, 'Writer' AS role
            FROM IsWriter          
          `,
  ]
      .filter((_, idx) => {
        if (idx === 0 && isCast) {
          return true;
        } else if (idx === 1 && isDirector) {
          return true;
        } else if (idx === 2 && isWriter) {
          return true;
        }
        return false;
      })
      .join(" UNION ")}
    ) 
    ${
      hasRole
          ? `
      ,all_roles AS (
          SELECT person_id, movie_id, 'Cast' AS role
          FROM IsCast
          UNION
          SELECT person_id, movie_id, 'Director' AS role
          FROM IsDirector
          UNION
          SELECT person_id, movie_id, 'Writer' AS role
          FROM IsWriter
      )
    `
          : ""
  }
    
    SELECT p.person_id, p.primary_name, p.birth_year, p.death_year,
           GROUP_CONCAT(DISTINCT CONCAT(m.movie_id, ',', m.start_year, ',', m.primary_title) ORDER BY m.start_year DESC SEPARATOR ';') AS movies,
           ${
      hasRole
          ? `GROUP_CONCAT(DISTINCT ar.role SEPARATOR ', ') AS roles,`
          : `GROUP_CONCAT(DISTINCT participated.role SEPARATOR ', ') AS roles,`
  }
           
           COUNT(*) OVER() AS full_count
    FROM Persons p
    JOIN participated on participated.person_id = p.person_id
    ${hasRole ? `JOIN all_roles ar on ar.person_id = p.person_id` : ""}
    JOIN Movies m on participated.movie_id = m.movie_id
    ${movie_name ? `and m.primary_title LIKE '%${movie_name}%'` : ""}
    WHERE 1
    ${name ? `AND p.primary_name LIKE '%${name}%'` : ""}
    ${min_birth_year ? `AND p.birth_year >= ${min_birth_year}` : ""}
    ${max_birth_year ? `AND p.birth_year <= ${max_birth_year}` : ""}
    ${
      min_death_year
          ? `AND p.death_year IS NOT NULL AND p.death_year >= ${min_death_year}`
          : ""
  }
    ${
      max_death_year
          ? `AND p.death_year IS NOT NULL AND p.death_year <= ${max_death_year}`
          : ""
  }
    GROUP BY p.person_id, p.primary_name, p.birth_year, p.death_year
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
    SELECT p.person_id, primary_name, birth_year, death_year, m.movie_id, primary_title, start_year, IC.characters,
    COUNT (DISTINCT m.movie_id) AS number_of_movies,
    COUNT (DISTINCT IC.movie_id) AS movies_acted,
    COUNT (DISTINCT ID.movie_id) AS movies_directed,
    COUNT (DISTINCT IW.movie_id) AS movies_written
    FROM Persons p
    LEFT JOIN IsKnownFor IKF ON p.person_id = IKF.person_id
    LEFT JOIN Movies m ON IKF.movie_id = m.movie_id
    LEFT JOIN IsCast IC on m.movie_id = IC.movie_id AND p.person_id = IC.person_id
    LEFT JOIN IsDirector ID on m.movie_id = ID.movie_id AND p.person_id = ID.person_id
    LEFT JOIN IsWriter IW on m.movie_id = IW.movie_id AND p.person_id = IW.person_id
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

async function related_actors(req, res) {
  const { person_id } = req.params;

  const baseQuery = `
    WITH all_roles AS (
        SELECT person_id, movie_id, 'Cast' AS role
        FROM IsCast
        UNION
        SELECT person_id, movie_id, 'Director' AS role
        FROM IsDirector
        UNION
        SELECT person_id, movie_id, 'Writer' AS role
        FROM IsWriter
    ), person_roles AS (
        SELECT all_roles.movie_id
        FROM all_roles
        WHERE person_id = '${person_id}'
    ),
     filtered_movies AS (
        SELECT movie_id, primary_title, start_year
        FROM Movies
        WHERE movie_id IN (
            SELECT ar.movie_id
            FROM all_roles ar
        )
    ),
     filtered_roles AS (
         SELECT *
        FROM all_roles ar
        WHERE ar.movie_id IN (SELECT pr.movie_id FROM person_roles pr)
    )
    SELECT
        p.person_id,
        p.primary_name,
        p.birth_year,
        p.death_year,
        GROUP_CONCAT(DISTINCT CONCAT(m.movie_id, ',', m.start_year, ',', m.primary_title) ORDER BY m.start_year DESC SEPARATOR ';') AS movies,
        GROUP_CONCAT(DISTINCT fr.role SEPARATOR ', ') AS roles
    FROM Persons p
    JOIN filtered_roles fr ON fr.person_id = p.person_id
    JOIN filtered_movies m ON fr.movie_id = m.movie_id
    GROUP BY p.person_id
    LIMIT 6;
  `;

  const responseHandler = (error, persons) => {
    if (error) {
      console.log(error);
      res.json({ error });
    } else if (persons) {
      res.json({ persons });
    }
  };

  if (person_id) {
    connection.query(baseQuery, responseHandler);
  } else {
    res.json({ error: "person_id not provided" });
  }
}

async function movie_rec(req, res) {
  const { movie_id } = req.params;

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
      LEFT JOIN Genres g on g.genre_id = hg.genre_id
    ),
    all_movie_genres AS (
      SELECT m.movie_id,
      GROUP_CONCAT(DISTINCT g.genre_name) AS genres
      FROM Movies m
      LEFT JOIN HasGenre hg ON hg.movie_id = m.movie_id
      LEFT JOIN Genres g on g.genre_id = hg.genre_id
      GROUP BY 1
    )
    SELECT m.movie_id, m.primary_title,
    m.start_year, m.overview,
    m.poster_path,
    a.genres AS genres,
    i.imdb_score, t.tmdb_score, r.rt_score AS rotten_tomatoes_score,
    ((i.imdb_score + t.tmdb_score + (r.rt_score / 10)) / 3) AS average_score
    FROM Movies m
    LEFT JOIN imdb_ratings i ON m.movie_id = i.movie_id
    LEFT JOIN tmdb_ratings t ON m.movie_id = t.movie_id
    LEFT JOIN rt_ratings r ON m.movie_id = r.movie_id
    LEFT JOIN all_movie_genres a ON m.movie_id = a.movie_id
    WHERE genres LIKE (SELECT genres FROM movie_genre)
    AND m.movie_id != '${movie_id}'
    ORDER BY average_score DESC
    LIMIT 6;
  `;

  const responseHandler = (error, results) => {
    if (error || results?.length === 0) {
      console.log(error);
      res.json({ error });
    } else if (results) {
      res.json({ movies: results });
    }
  };

  connection.query(baseQuery, responseHandler);
}

async function movie_person_rec(req, res) {
  const { person_id } = req.params;

  const baseQuery = `
    WITH cast_filter AS (
      SELECT DISTINCT(m.primary_title) AS movies,
      c.movie_id,
      m.start_year,
      m.poster_path,
      m.overview
      FROM Persons p
      LEFT JOIN IsCast c ON p.person_id = c.person_id
      LEFT JOIN Movies m ON c.movie_id = m.movie_id
      WHERE p.person_id LIKE '${person_id}'
    ), directed_filter AS (
      SELECT DISTINCT(m.primary_title) AS movies,
      c.movie_id,
      m.start_year,
      m.poster_path,
      m.overview
      FROM Persons p
      LEFT JOIN IsDirector c ON p.person_id = c.person_id
      LEFT JOIN Movies m ON c.movie_id = m.movie_id
      WHERE p.person_id LIKE '${person_id}'
    ), written_filter AS (
      SELECT DISTINCT(m.primary_title) AS movies,
      c.movie_id,
      m.start_year,
      m.poster_path,
      m.overview
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
    SELECT m.movie_id, m.movies AS primary_title, m.poster_path,
    m.start_year, m.overview,
    i.imdb_score, t.tmdb_score, r.rt_score AS rotten_tomatoes_score,
    ((i.imdb_score + t.tmdb_score + (r.rt_score / 10)) / 3) AS average_score
    FROM all_movies m
    LEFT JOIN imdb_ratings i ON m.movie_id = i.movie_id
    LEFT JOIN tmdb_ratings t ON m.movie_id = t.movie_id
    LEFT JOIN rt_ratings r ON m.movie_id = r.movie_id
    ORDER BY average_score DESC
    LIMIT 6;
  `;

  const responseHandler = (error, results) => {
    if (error || results?.length === 0) {
      console.log(error);
      res.json({ error });
    } else if (results) {
      res.json({ movies: results });
    }
  };

  connection.query(baseQuery, responseHandler);
}

module.exports = {
  home,
  movies,
  movie,
  movie_cast,
  movie_director_and_writer,
  all_genres,
  persons,
  person,
  movie_rec,
  movie_person_rec,
  related_actors,
};
