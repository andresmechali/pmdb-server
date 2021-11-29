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

  const baseQuery = ` ${ /* Need to add the Award but taking 2s it's exactly like the List of movie known just need the table PersonNomination */'' }
  create view knownfor as(
    With Movie(movie_id, movie) as(
    select movie_id, primary_title
    from Movies),
    
    Person(person_id, firstname, Surname, Born, Death) as(
        select Distinct p.person_id, LEFT(primary_name,LOCATE(' ',primary_name) - 1) as Firstname,RIGHT(primary_name,length(primary_name) - LOCATE(' ',primary_name) ) as Surname, birth_year, death_year
        from Persons p),
    
    knownfor(person_id,firstname,surname,Born, Death, movie,movie_id) as(
    select p.person_id,firstname,Surname ,Born, Death,movie,m.movie_id
    from Person p
        Join IsKnownFor i on i.person_id= p.person_id
        join Movie m on i.movie_id = m.movie_id)
    
    select * from knownfor);
    
    CREATE VIEW FILTERED as (
    SELECT person_id,firstname,Surname ,Born, Death, GROUP_CONCAT( distinct movie separator " / ") as List_movie_knownfor FROM knownfor GROUP BY firstname,Surname ,Born, Death
    );
    select * from FILTERED;
    
    create view grossingA as(
    With Movie(movie_id, movie, grossing) as(
    select movie_id, primary_title,lifetime_grossing
    from Movies),
    
    Person(person_id, firstname, Surname) as(
        select Distinct p.person_id, LEFT(primary_name,LOCATE(' ',primary_name) - 1) as Firstname,RIGHT(primary_name,length(primary_name) - LOCATE(' ',primary_name) ) as Surname
        from Persons p),
    
    grossing(person_id,firstname,surname,movieA,movie_id,grossing) as(
    select Distinct p.person_id,firstname,Surname ,movie,m.movie_id,MAX(grossing)
    from Person p
        Join IsCast i on i.person_id= p.person_id
        join Movie m on i.movie_id = m.movie_id
    Group BY firstname,Surname)
    
    select * from grossing);
    
    create view grossingD as(
    With Movie(movie_id, movie, grossing) as(
    select movie_id, primary_title,lifetime_grossing
    from Movies),
    
    Person(person_id, firstname, Surname) as(
        select Distinct p.person_id, LEFT(primary_name,LOCATE(' ',primary_name) - 1) as Firstname,RIGHT(primary_name,length(primary_name) - LOCATE(' ',primary_name) ) as Surname
        from Persons p),
    
    grossing(person_id,firstname,surname,movie,movie_id,grossing) as(
    select Distinct p.person_id,firstname,Surname ,movie,m.movie_id,MAX(grossing)
    from Person p
        Join IsDirector i on i.person_id= p.person_id
        join Movie m on i.movie_id = m.movie_id
    Group BY firstname,Surname)
    
    select * from grossing);
    
    select D.movie, D.grossing
    from grossingD D;
    
    create view grossingW as(
    With Movie(movie_id, movie, grossing) as(
    select movie_id, primary_title,lifetime_grossing
    from Movies),
    
    Person(person_id, firstname, Surname) as(
        select Distinct p.person_id, LEFT(primary_name,LOCATE(' ',primary_name) - 1) as Firstname,RIGHT(primary_name,length(primary_name) - LOCATE(' ',primary_name) ) as Surname
        from Persons p),
    
    grossing(person_id,firstname,surname,movie,movie_id,grossing) as(
    select Distinct p.person_id,firstname,Surname ,movie,m.movie_id,MAX(grossing)
    from Person p
        Join IsWriter i on i.person_id= p.person_id
        join Movie m on i.movie_id = m.movie_id
    Group BY firstname,Surname)
    
    select * from grossing);
    
    select k.firstname,k.Surname ,k.Born, k.Death,k.List_movie_knownfor , A.movieA as Highest_Grossing_Movie_Acted_in, D.movie as Highest_Grossing_Movie_Directed, W.movie as Highest_Grossing_Movie_Written
    from FILTERED k
        JOIN grossingA A ON  k.person_id = A.person_id
        JOIN grossingD D ON  k.person_id = D.person_id
        JOIN grossingW W ON  k.person_id = W.person_id;
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

async function actors_work_with(req, res) {

  const page = req.params.page || 1;
  const limit = 10;
  const offset = limit * (page - 1);

  const name_filter = req.query.name_filter ? req.query.name_filter : '';


  const baseQuery = ` ${ /* This is just filtered based on Actors name and it taking too much time to execute ! i don't know the purpose of fitered the type since we are outputing the number of film acted / directed / written */'' }
  
  create view acted as( ${ /* count movie Acted */'' }

    With Movie(movie_id, movie) as(
    select movie_id, primary_title
    from Movies),
    
    Person(person_id, firstname, Surname, Born, Death) as(
        select Distinct p.person_id, LEFT(primary_name,LOCATE(' ',primary_name) - 1) as Firstname,RIGHT(primary_name,length(primary_name) - LOCATE(' ',primary_name) ) as Surname, birth_year, death_year
        from Persons p),
    
    acted(person_id,firstname,surname,movieA,movie_id ,number_movies_acted_in, Born, Death) as(
    select Distinct p.person_id,firstname,Surname ,movie,m.movie_id, count(distinct  m.movie_id), Born,Death
    from Person p
        Join IsCast i on i.person_id= p.person_id
        join Movie m on i.movie_id = m.movie_id
    Group BY firstname,Surname)
    
    select * from acted
    );
    
    create view directed as( ${ /* count movie Directed */'' }
    
    With Movie(movie_id, movie) as(
    select movie_id, primary_title
    from Movies),
    
    Person(person_id, firstname, Surname, Born, Death) as(
        select Distinct p.person_id, LEFT(primary_name,LOCATE(' ',primary_name) - 1) as Firstname,RIGHT(primary_name,length(primary_name) - LOCATE(' ',primary_name) ) as Surname, birth_year, death_year
        from Persons p),
    
    directed(person_id,firstname,surname,movieA,movie_id ,number_movies_directed, Born, Death) as(
    select Distinct p.person_id,firstname,Surname ,movie,m.movie_id, count(distinct  m.movie_id) -1, Born,Death
    from Person p
        Join IsDirector i on i.person_id= p.person_id
        join Movie m on i.movie_id = m.movie_id
    Group BY firstname,Surname)
    select * from directed
    );
    
    
    create view written as( ${ /* count movie Written */'' }
    
    With Movie(movie_id, movie) as(
    select movie_id, primary_title
    from Movies),
    
    Person(person_id, primary_name, firstname, Surname, Born, Death) as(
        select Distinct p.person_id, primary_name, LEFT(primary_name,LOCATE(' ',primary_name) - 1) as Firstname,RIGHT(primary_name,length(primary_name) - LOCATE(' ',primary_name) ) as Surname, birth_year, death_year
        from Persons p),
    
    written(person_id,primary_name,firstname,surname,movieA,movie_id ,number_movies_written, Born, Death) as(
    select Distinct p.person_id,primary_name,firstname,Surname ,movie,m.movie_id, count(distinct  m.movie_id)-1, Born,Death
    from Person p
        Join IsWriter i on i.person_id= p.person_id
        join Movie m on i.movie_id = m.movie_id
    Group BY firstname,Surname)
    select * from written
    );
    
    create view p6 as( ${ /* Joining Evrything */'' }
    select a.person_id,w.primary_name,a.firstname ,a.surname,a.Born, a.Death, a.movie_id, k.List_movie_knownfor ,a.number_movies_acted_in, d.number_movies_directed, w.number_movies_written
    from FILTERED k
    JOIN acted a on k.person_id =a.person_id
    JOIN directed d on k.person_id = d.person_id
    JOIN written w on k.person_id = w.person_id);
    
    
    With VID(movie_id) As(
    select distinct i.movie_id
    from  IsCast i Join Persons p on i.person_id = p.person_id
    WHERE p.primary_name = ${name_filter} ),
    
    Person_movie(primary_name, movie_id) As(
    select p.primary_name,  i.movie_id
    from Persons p
    JOIN IsCast i on i.person_id = p.person_id ${ /* If we want to do the same thing with the different types just copy past this and add and if statement we changement the IsCast with Isdirected and IsWritten EASY !  */'' }
    Join Movies M ON i.movie_id = i.movie_id
    )
    
    select  p.primary_name ,p.firstname ,p.surname,p.Born, p.Death, p.List_movie_knownfor ,p.number_movies_acted_in, p.number_movies_directed, p.number_movies_written
    from p6 p , Person_movie pm, VID v
    Where pm.movie_id in (v.movie_id); ${ /* This is just an example running but with duplicate we need to use distinct or Group By but it taking infinite time -- replace with below */'' }
  LIMIT ${limit}
  OFFSET ${offset};
  `; 

  `
  select Distinct p.primary_name ,p.firstname ,p.surname,p.Born, p.Death, p.List_movie_knownfor ,p.number_movies_acted_in, p.number_movies_directed, p.number_movies_written
  from p6 p, Person_movie pm, VID v
  where pm.movie_id IN (v.movie_id) and not exists(
  select p.primary_name ,p.firstname ,p.surname,p.Born, p.Death, p.List_movie_knownfor ,p.number_movies_acted_in, p.number_movies_directed, p.number_movies_written
  from p6 p, Person_movie pm, VID v
  WHERE p.primary_name = ${name_filter} ` ${ /* Real Query but it is not running add distinct and delete the person choosen taking infinite time */'' }

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