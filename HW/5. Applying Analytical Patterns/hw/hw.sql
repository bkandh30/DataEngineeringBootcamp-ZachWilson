-- 1. Tracking Players’ State Changes
-- This query tracks state changes for players based on their seasons and activity.
WITH player_changes AS (
    SELECT 
        p.player_name,
        p.current_season,
        p.is_active,
        COALESCE(
            (SELECT MAX(current_season) FROM players WHERE player_name = p.player_name), 
            (SELECT MIN(current_season) - 1 FROM players WHERE player_name = p.player_name)
        ) AS last_season
    FROM players AS p
)
SELECT
    p.player_name,
    p.current_season AS current_year,
    CASE
        WHEN p.last_season IS NULL AND p.is_active = TRUE THEN 'New'
        WHEN p.last_season IS NOT NULL AND p.is_active = TRUE THEN 'Continued Playing'
        WHEN p.last_season IS NOT NULL AND p.is_active = FALSE THEN 'Retired'
        WHEN p.last_season IS NULL AND p.is_active = FALSE THEN 'Stayed Retired'
        WHEN p.is_active = TRUE AND p.last_season IS NOT NULL THEN 'Returned from Retirement'
    END AS player_state,
    p.is_active,
    ARRAY[p.current_season] AS years_active
FROM player_changes AS p
ORDER BY p.player_name;

-- 2. Aggregation with GROUPING SETS
-- This query uses GROUPING SETS to perform aggregations for player-team, player-season, and team-level data.
-- Separate CTEs for each aggregation type are used for clarity.

-- Player and Team Aggregation (Top scorer per team)
WITH player_team_aggregation AS (
    SELECT
        player_name,
        team_id,
        SUM(pts) AS total_points
    FROM game_details
    GROUP BY player_name, team_id
    ORDER BY total_points DESC
    LIMIT 1
),

-- Player and Season Aggregation (Top scorer per season)
player_season_aggregation AS (
    SELECT
        player_name,
        season,
        SUM(pts) AS total_points
    FROM game_details
    GROUP BY player_name, season
    ORDER BY total_points DESC
    LIMIT 1
),

-- Team Aggregation (Top team by wins)
team_aggregation AS (
    SELECT 
        team_id,
        COUNT(*) AS total_wins
    FROM game_details
    WHERE home_team_wins = 1  -- Assuming home_team_wins indicates a win for the team
    GROUP BY team_id
    ORDER BY total_wins DESC
    LIMIT 1
)

-- Combining Results from All Aggregations
SELECT * FROM player_team_aggregation
UNION ALL
SELECT * FROM player_season_aggregation
UNION ALL
SELECT * FROM team_aggregation;

-- 3. Player with Most Points for a Single Team
-- Identifies the player who scored the most points for a single team.
SELECT 
    player_name,
    team_id,
    SUM(pts) AS total_points
FROM game_details
GROUP BY player_name, team_id
ORDER BY total_points DESC
LIMIT 1;

-- 4. Player with Most Points in a Single Season
-- Identifies the player who scored the most points in a single season.
SELECT 
    player_name,
    season,
    SUM(pts) AS total_points
FROM game_details
GROUP BY player_name, season
ORDER BY total_points DESC
LIMIT 1;

-- 5. Team with Most Total Wins
-- Identifies the team with the most wins. Assumption is that 'home_team_wins' indicates a win.
SELECT 
    team_id,
    COUNT(*) AS total_wins
FROM game_details
WHERE home_team_wins = 1  -- Assumed win condition
GROUP BY team_id
ORDER BY total_wins DESC
LIMIT 1;

-- 6. Window Function for Most Wins in a 90-Game Stretch
-- This query calculates the most wins in a rolling 90-game stretch for each team.
WITH all_games AS (
    SELECT
        g.game_date_est,
        g.game_id,
        g.home_team_id AS team_id,
        g.home_team_wins AS win
    FROM public.games AS g
    UNION ALL
    SELECT
        g.game_date_est,
        g.game_id,
        g.visitor_team_id AS team_id,
        1 - g.home_team_wins AS win
    FROM public.games AS g
),
rolling_wins AS (
    SELECT
        team_id,
        game_date_est,
        SUM(win) OVER (
            PARTITION BY team_id
            ORDER BY game_date_est
            ROWS 89 PRECEDING
        ) AS wins_in_90_games
    FROM all_games
)
SELECT
    team_id,
    MAX(wins_in_90_games) AS max_wins_90_game_stretch
FROM rolling_wins
GROUP BY team_id
ORDER BY max_wins_90_game_stretch DESC
LIMIT 1;

-- 7. Longest Streak of LeBron James Scoring Over 10 Points
-- This query calculates LeBron James' longest streak of scoring over 10 points in consecutive games.
WITH streaks AS (
    SELECT
        gd.player_name,
        gd.game_date_est,
        gd.pts,
        ROW_NUMBER() OVER (PARTITION BY gd.player_name ORDER BY gd.game_date_est) AS streak_num
    FROM game_details AS gd
    WHERE gd.player_name = 'LeBron James' AND gd.pts > 10
),
longest_streaks AS (
    SELECT
        player_name,
        (streak_num - ROW_NUMBER() OVER (PARTITION BY player_name ORDER BY game_date_est)) AS streak_group
    FROM streaks
)
SELECT
    player_name,
    COUNT(*) AS longest_streak
FROM longest_streaks
GROUP BY player_name, streak_group
ORDER BY longest_streak DESC
LIMIT 1;
