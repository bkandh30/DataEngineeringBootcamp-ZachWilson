-- 1. Query to Deduplicate game_details
-- Explanation:
-- Using ROW_NUMBER() to remove duplicates based on (game_id, team_id, player_id).
-- LEFT JOIN ensures rows in `game_details` remain even if `games` might be missing data.
WITH deduped AS (
    SELECT
        g.game_date_est,
        g.season,
        g.home_team_id,
        gd.*,
        ROW_NUMBER() OVER (
            PARTITION BY gd.game_id, gd.team_id, gd.player_id
            ORDER BY g.game_date_est
        ) AS row_num
    FROM game_details gd
    LEFT JOIN games g
    ON gd.game_id = g.game_id
)

SELECT
    game_date_est AS dim_game_date,
    season AS dim_season,
    team_id AS dim_team_id,
    player_id AS dim_player_id,
    player_name AS dim_player_name,
    start_position AS dim_start_position,
    (team_id = home_team_id) AS dim_is_playing_at_home,
    COALESCE(POSITION('DNP' IN comment), 0) > 0  AS dim_did_not_play,
    COALESCE(POSITION('DND' IN comment), 0) > 0  AS dim_did_not_dress,
    COALESCE(POSITION('NWT' IN comment), 0) > 0  AS dim_not_with_team,
    CAST(SPLIT_PART(min, ':', 1) AS REAL)
      + CAST(SPLIT_PART(min, ':', 2) AS REAL) / 60 AS m_minutes,
    fgm AS m_fgm,
    fga AS m_fga,
    fg3m AS m_fg3m,
    fg3a AS m_fg3a,
    ftm AS m_ftm,
    fta AS m_fta,
    oreb AS m_oreb,
    dreb AS m_dreb,
    reb AS m_reb,
    ast AS m_ast,
    stl AS m_stl,
    blk AS m_blk,
    "TO" AS m_turnovers,
    pf AS m_pf,
    pts AS m_pts,
    plus_minus AS m_plus_minus
FROM deduped
WHERE row_num = 1;

-- 2. DDL for user_devices_cumulated table
-- Explanation:
-- Table schema for storing device activity arrays. BIGINT is used here,
-- as it may be more suitable for user_id or device_id than NUMERIC.
CREATE TABLE user_devices_cumulated (
    user_id BIGINT,
    device_id BIGINT,
    browser_type TEXT,
    date DATE,
    device_activity_datelist DATE[],
    PRIMARY KEY (user_id, device_id, browser_type)
);

-- 3. Cumulative query to generate device_activity_datelist from events
-- Explanation:
--   1) Collect today's rows from events (grouped by user/device/browser).
--   2) Collect yesterday’s snapshot from user_devices_cumulated.
--   3) Combine arrays via FULL OUTER JOIN, then insert new records.
INSERT INTO user_devices_cumulated
WITH today AS (
    SELECT
        e.user_id,
        DATE(e.event_time) AS date,
        e.device_id,
        d.browser_type
    FROM events e
    LEFT JOIN devices d
    ON e.device_id = d.device_id
    WHERE DATE(e.event_time) = DATE('2023-01-31')
    AND e.user_id IS NOT NULL
    AND e.device_id IS NOT NULL
    GROUP BY e.user_id, e.device_id, d.browser_type, DATE(e.event_time)
), yesterday AS (
    SELECT
        user_id,
        device_id,
        browser_type,
        date,
        device_activity_datelist
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-30')
)
SELECT
    COALESCE(t.user_id, y.user_id) AS user_id,
    COALESCE(t.device_id, y.device_id) AS device_id,
    COALESCE(t.browser_type, y.browser_type) AS browser_type,
    COALESCE(t.date, y.date + 1) AS date,
    CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.date]
        WHEN t.date IS NULL THEN y.device_activity_datelist
        ELSE y.device_activity_datelist || ARRAY[t.date]
    END AS device_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y
ON t.user_id = y.user_id
AND t.device_id = y.device_id
AND t.browser_type = y.browser_type;

-- 4. datelist_int generation query
-- Explanation:
--   1) Pull final user_devices rows from snapshot date (2023-01-31).
--   2) Generate a day-by-day series for the month.
--   3) CROSS JOIN each device with each day to compute a bit-value if active.
--   4) SUM bit-values to produce a 32-bit integer of active days.
WITH users_devices AS (
    SELECT
        *
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-31')
), series AS (
    SELECT
        generate_series('2023-01-02'::date, '2023-01-31'::date, INTERVAL '1 day') AS series_date
), place_holder_ints AS (
    SELECT
        ud.*,
        s.series_date,
        CASE
            WHEN ud.device_activity_datelist @> ARRAY[DATE(s.series_date)]
            THEN CAST(
                POW(
                    2,
                    LEAST(32, GREATEST(0, 32 - (ud.date - DATE(s.series_date))))
                )
            AS BIGINT)
            ELSE 0
        END AS placeholder_int_value
    FROM users_devices ud
    CROSS JOIN series s
)
SELECT
    user_id,
    device_id,
    browser_type,
    device_activity_datelist,
    CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32)) AS datelist_int
FROM place_holder_ints p
GROUP BY user_id, device_id, browser_type, device_activity_datelist;

-- 5. DDL for hosts_cumulated
-- Explanation:
-- Schema for storing host-level date arrays. The (host, month_start) PK
-- ensures uniqueness per month.
CREATE TABLE hosts_cumulated (
    host TEXT,
    month_start DATE,
    host_activity_datelist DATE[],
    PRIMARY KEY (host, month_start)
);

-- 6. Incremental query to generate host_activity_datelist
-- Explanation:
--   1) Get today's active hosts by date.
--   2) Select existing monthly record in hosts_cumulated.
--   3) FULL OUTER JOIN + array concatenation of dates.
--   4) Upsert via ON CONFLICT.
INSERT INTO hosts_cumulated
WITH today AS (
    SELECT
        host,
        DATE(event_time) AS date
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-10')
    GROUP BY host, DATE(event_time)
), yesterday AS (
    SELECT
        host,
        month_start,
        host_activity_datelist
    FROM hosts_cumulated
    WHERE month_start = DATE('2023-01-01')
)
SELECT
    COALESCE(t.host, y.host) AS host,
    COALESCE(y.month_start, DATE_TRUNC('month', t.date)::date) AS month_start,
    CASE
        WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.date]
        WHEN t.date IS NULL THEN y.host_activity_datelist
        ELSE y.host_activity_datelist || ARRAY[t.date]
    END AS host_activity_datelist
FROM today t
FULL OUTER JOIN yesterday y
ON t.host = y.host
ON CONFLICT (host, month_start)
DO
    UPDATE SET host_activity_datelist = EXCLUDED.host_activity_datelist;

-- 7. DDL for host_activity_reduced
-- Explanation:
-- Storing arrays for hits and unique visitors at a monthly granularity.
CREATE TABLE host_activity_reduced (
    host TEXT,
    month_start DATE,
    hit_array NUMERIC[],
    unique_visitors NUMERIC[],
    PRIMARY KEY (host, month_start)
);

-- 8. Incremental query for host_activity_reduced
-- Explanation:
--   1) Aggregate today's hits/visitors per host.
--   2) Pull existing monthly record.
--   3) Combine arrays via FULL OUTER JOIN.
--   4) Upsert via ON CONFLICT.
INSERT INTO host_activity_reduced
WITH today AS (
    SELECT
        host,
        DATE(event_time) AS date,
        COUNT(DISTINCT user_id) AS unique_visitors,
        COUNT(*) AS hits
    FROM events
    WHERE DATE(event_time) = DATE('2023-01-10')
    GROUP BY host, DATE(event_time)
), yesterday AS (
    SELECT
        host,
        month_start,
        unique_visitors,
        hit_array
    FROM host_activity_reduced
    WHERE month_start = DATE('2023-01-01')
)
SELECT
    COALESCE(t.host, y.host) AS host,
    COALESCE(y.month_start, DATE_TRUNC('month', t.date)::date) AS month_start,
    CASE
        WHEN y.unique_visitors IS NOT NULL THEN
            y.unique_visitors || ARRAY[COALESCE(t.unique_visitors, 0)]
        WHEN y.unique_visitors IS NULL THEN
            ARRAY_FILL(
                0,
                [GREATEST(1, COALESCE(date - DATE(DATE_TRUNC('month', date)), 0))]
            ) || ARRAY[COALESCE(t.unique_visitors, 0)]
    END AS unique_visitors,
    CASE
        WHEN y.hit_array IS NOT NULL THEN
            y.hit_array || ARRAY[COALESCE(t.hits, 0)]
        WHEN y.hit_array IS NULL THEN
            ARRAY_FILL(
                0,
                [GREATEST(1, COALESCE(date - DATE(DATE_TRUNC('month', date)), 0))]
            ) || ARRAY[COALESCE(t.hits, 0)]
    END AS hit_array
FROM today t
FULL OUTER JOIN yesterday y
ON t.host = y.host
ON CONFLICT (host, month_start)
DO
    UPDATE SET unique_visitors = EXCLUDED.unique_visitors,
        hit_array = EXCLUDED.hit_array;
