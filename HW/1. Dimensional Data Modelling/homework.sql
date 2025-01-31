-- 1. DDL for actors table

-- Films struct

-- DROP TYPE films

CREATE TYPE films AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
)

-- Quality class enum

-- DROP TYPE quality_class

CREATE TYPE quality_class AS ENUM (
	'star', 'good', 'average', 'bad'
)

-- Actors table

-- DROP TABLE actors

CREATE TABLE actors (
	actor TEXT,
	actorid TEXT,
	current_year INTEGER,
	films films[],
	quality_class quality_class,
	is_active BOOLEAN,
	PRIMARY KEY (actorid, current_year)
)

-- 2. Cumulative Table Generation Query
INSERT INTO actors
WITH last_year AS (
	SELECT *
	FROM actors
	WHERE current_year = 2020
),
	this_year AS (
	SELECT *
	FROM actor_films
	WHERE year = 2021
),
	this_year_film_and_ratings AS (
	SELECT actorid,
		actor,
		year,
		ARRAY_AGG(ROW(
				ty.film,
				ty.votes,
				ty.rating,
				ty.filmid)::films) as current_films,
		AVG(rating) AS average_rating
	FROM this_year AS ty
	GROUP BY actorid, actor, year
)


SELECT
	COALESCE(ly.actorid,ty.actorid) AS actorid,
	COALESCE(ly.actor,ty.actor) AS actor,
	COALESCE(ly.current_year+1,ty.year) AS current_year,
	CASE
		WHEN ly.current_year IS NULL THEN ty.current_films
		WHEN ty.year IS NULL THEN ly.films
		ELSE ly.films || ty.current_films
	END::films[] AS films,
	CASE
		WHEN ty.average_rating IS NULL
		THEN ly.quality_class
		ELSE
			CASE
				WHEN ty.average_rating > 8 THEN 'star'
				WHEN ty.average_rating > 7 THEN 'good'
				WHEN ty.average_rating > 6 THEN 'average'
				ELSE 'bad'
			END::quality_class
	END::quality_class,
	CASE
		WHEN ty IS NULL
		THEN False
		ELSE True
	END AS is_active
FROM this_year_film_and_ratings ty
FULL OUTER JOIN last_year ly
ON ly.actorid = ty.actorid


--- 3. DDL for actors_history_scd

CREATE TABLE actors_history_scd (
	actorid TEXT,
	is_active BOOLEAN,
	quality_class quality_class,
	current_year INTEGER,
	start_date INTEGER,
	end_date INTEGER
)


-- 4. Backfill query for actors_history_scd


INSERT INTO actors_history_scd
WITH with_previous AS (
	SELECT
		actorid,
		current_year,
		quality_class,
		is_active,
		LAG(quality_class, 1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
		LAG(is_active,1) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
	FROM actors
	WHERE current_year <= 2021
),
	with_indicators AS (
		SELECT *,
			CASE
				WHEN quality_class <> previous_quality_class THEN 1
				WHEN is_active <> previous_is_active THEN 1
				ELSE 0
			END as change_indicator
		FROM with_previous	
),
	with_streaks AS (
		SELECT *,
			SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_identifier
		FROM with_indicators
)

SELECT actorid,
	is_active,
	quality_class,
	2024 as current_year,
	MIN(current_year) as start_date,
	MAX(current_year) as end_date
FROM with_streaks
GROUP BY actorid, streak_identifier, is_active, quality_class
ORDER BY actorid, start_date



-- 5. Incremental query for actors_history_scd

CREATE TYPE actors_scd_type AS (
	quality_class quality_class,
	is_active BOOLEAN,
	start_date INTEGER,
	end_date INTEGER
)


WITH last_year_scd AS (
	SELECT * 
	FROM actors_history_scd
	WHERE current_year = 2024
	AND end_date = 2024
), historical_scd AS (
	SELECT actorid,
		quality_class,
		is_active,
		start_date,
		end_date
	FROM actors_history_scd
	WHERE current_year = 2024
	AND end_date < 2024
), this_year_data AS (
	SELECT *
	FROM actors
	WHERE current_year = 2025
),
	unchanged_records AS (
		SELECT
			COALESCE (ty.actorid,ly.actorid) AS actorid,
			COALESCE (ty.quality_class,ly.quality_class) AS quality_class,
			COALESCE (ty.is_active,ly.is_active) AS is_active,
			ly.start_date,
			ty.current_year AS end_date
		FROM this_year_data ty
		JOIN last_year_scd ly
		ON ty.actorid = ly.actorid
		WHERE ty.quality_class = ly.quality_class
		AND ty.is_active = ly.is_active
),
	changed_records AS (
		SELECT
			COALESCE (ty.actorid,ly.actorid) AS actorid,
			UNNEST(ARRAY[
				ROW(
					ly.quality_class,
					ly.is_active,
					ly.start_date,
					ly.end_date
				)::actors_scd_type,
				ROW(
					ty.quality_class,
					ty.is_active,
					ty.current_year,
					ty.current_year
				)::actors_scd_type
			]) AS records
		FROM this_year_data ty
		LEFT JOIN last_year_scd ly
		ON ty.actorid = ly.actorid
		WHERE (ty.quality_class <> ly.quality_class
		OR ty.is_active <> ly.is_active)
),
	unnested_changed_records AS (
		SELECT actorid,
			(records::actors_scd_type).quality_class,
			(records::actors_scd_type).is_active,
			(records::actors_scd_type).start_date,
			(records::actors_scd_type).end_date
		FROM changed_records
),
	new_records AS (
		SELECT
			ty.actorid,
			ty.quality_class,
			ty.is_active,
			ty.current_year AS start_date,
			ty.current_year AS end_date
		FROM this_year_data ty
		LEFT JOIN last_year_scd AS ly
		ON ty.actorid = ly.actorid
		WHERE ly.actorid IS NULL
)

SELECT * FROM historical_scd

UNION ALL

SELECT * FROM unchanged_records

UNION ALL

SELECT * FROM unnested_changed_records

UNION ALL

SELECT * FROM new_records

ORDER BY end_date DESC