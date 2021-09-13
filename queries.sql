------------------------------------------------------------------------------------------------------------------------------------------------
-- Write the DDL statements of two tables (seasons & teams) defining which columns are required to store the downloaded data.
------------------------------------------------------------------------------------------------------------------------------------------------

--teams DDL

CREATE TABLE pl_teams (
	id INT,
	name TEXT,
	shortName TEXT,
	tla TEXT,
	crestUrl TEXT,
	address TEXT,
	phone TEXT,
	website TEXT,
	email TEXT,
	founded INT,
	clubColors TEXT,
	venue TEXT,
	lastUpdated TEXT,
	area_id BIGINT,
	area_name TEXT,
	PRIMARY KEY (id)
);




--Seasons DDL

CREATE TABLE pl_seasons (
	id INT,
	startDate TEXT,
	endDate TEXT,
	currentMatchday INT,
	winner INT,
	winner_id INT,
	winner_name TEXT,
	winner_shortName TEXT,
	winner_tla TEXT,
	PRIMARY KEY (id),
	foreign key(winner_id) references pl_teams(id)
);
------------------------------------------------------------------------------------------------------------------------------------------------


------------------------------------------------------------------------------------------------------------------------------------------------
--Create a SQL query that will output all the teams that have won at least 3 times the Premier League since 2000-08-19.
--Show the team’s name and how many titles it has won.
------------------------------------------------------------------------------------------------------------------------------------------------


SELECT   winner_name,
         Count(winner_id) filter (WHERE startdate > '2000-08-19') AS won_titles
FROM     pl_seasons ps
GROUP BY winner_name
HAVING   won_titles > 3





------------------------------------------------------------------------------------------------------------------------------------------------
-----Create a SQL query that will list the name of the teams that have won the Premier League, the start date of the winning season(s)
---- and the end date of the winning season(s), considering that if a team wins consecutive seasons the winning dates should range from
---- the start of the first winning season until the end of the last consecutive winning season.
------------------------------------------------------------------------------------------------------------------------------------------------


WITH cte
     AS (
     	SELECT 	*,
            	Lag(winner_id) OVER (ORDER BY startdate) AS last_winner
       	FROM 
       		pl_seasons ps
        )
        
        
SELECT id,
       Cast(winner_id AS INTEGER) winner_id,
       winner_name,
       Min(startdate)             AS winning_streak_start,
       Max(enddate)               AS winning_streak_end,
       Count(winner_id)           AS consec_season_win
FROM   
	(
		SELECT id,
               startdate,
               enddate,
               winner_id,
               winner_name,
               Sum(CASE
                     WHEN last_winner = winner_id THEN 0
                     ELSE 1
                   END)
                 OVER (ORDER BY startdate ) AS grp
        FROM   
        	cte
     )
GROUP BY 
	winner_name,
    grp
ORDER BY 
	startdate 

------------------------------------------------------------------------------------------------------------------------------------------------