-- Create processed events aggregated table
CREATE TABLE processed_events_ip_aggregated (
    event_window_timestamp TIMESTAMP(3),
    host VARCHAR,
    ip VARCHAR,
    num_hits BIGINT
);


-- Question: What is the average number of web events of a session from a user on Tech Creator?
-- Answer: The average number of hits for most IP addresses is 1 or 2, but there are multiple users that have multiple hits.

-- | ip               | host                      | event_window_timestamp     | average_num_hits |
-- |------------------|--------------------------|----------------------------|------------------|
-- | 172.59.183.90    | bootcamp.techcreator.io  | 2025-02-06 23:10:54.016    | 2                |
-- | 104.176.23.168   | bootcamp.techcreator.io  | 2025-02-06 23:09:40.878    | 2                |
-- | 176.186.182.90   | bootcamp.techcreator.io  | 2025-02-06 23:10:10.308    | 1                |
-- | 67.162.153.74    | bootcamp.techcreator.io  | 2025-02-06 23:10:09.002    | 9                |
-- | 136.52.6.217     | bootcamp.techcreator.io  | 2025-02-06 23:09:53.718    | 6                |
-- | 71.128.11.123    | bootcamp.techcreator.io  | 2025-02-06 23:08:47.546    | 1                |
-- | 191.156.248.93   | bootcamp.techcreator.io  | 2025-02-06 23:10:18.152    | 2                |
-- | 197.255.164.153  | bootcamp.techcreator.io  | 2025-02-06 23:08:46.053    | 1                |
-- | 104.28.55.230    | bootcamp.techcreator.io  | 2025-02-06 23:09:49.267    | 1                |
-- | 52.35.125.236    | bootcamp.techcreator.io  | 2025-02-06 23:10:08.093    | 1                |

SELECT ip,
       host,
       CAST(AVG(num_hits) AS INTEGER) AS average_num_hits
FROM processed_events_ip_aggregated
WHERE host LIKE '%techcreator.io'  -- Ensure we're only analyzing Tech Creator domains
GROUP BY ip, host
ORDER BY average_num_hits DESC;

-- Question: Compare results between different hosts (zachwilson.techcreator.io, zachwilson.tech, lulu.techcreator.io)
-- Answer: dataexpert.io, bootcamp.techcreator.io both have the most hits to their site, www.techcreator.io has moderate activity, whereas the other sites have little to no activity

-- | ip                    | host                        | event_window_timestamp     | average_num_hits |
-- |-----------------------|----------------------------|----------------------------|------------------|
-- | 47.156.230.58        | bootcamp.techcreator.io     | 2025-02-06 23:11:27.574    | 1                |
-- | 184.64.33.51         | www.dataexpert.io           | 2025-02-06 23:09:45.022    | 2                |
-- | 69.160.114.50        | www.dataexpert.io           | 2025-02-06 23:09:14.403    | 2                |
-- | 172.59.183.96        | bootcamp.techcreator.io     | 2025-02-06 23:10:54.016    | 2                |
-- | 104.176.23.168       | bootcamp.techcreator.io     | 2025-02-06 23:09:40.878    | 2                |
-- | 14.99.167.142        | www.dataexpert.io           | 2025-02-06 23:08:00.7      | 1                |
-- | 109.146.196.140      | www.dataexpert.io           | 2025-02-06 23:08:46.701    | 1                |
-- | 176.186.182.90       | bootcamp.techcreator.io     | 2025-02-06 23:10:10.308    | 1                |
-- | 170.199.151.216      | www.dataexpert.io           | 2025-02-06 23:08:45.768    | 2                |
-- | 67.162.153.74        | bootcamp.techcreator.io     | 2025-02-06 23:10:09.002    | 9                |
-- | 79.213.143.230       | www.dataexpert.io           | 2025-02-06 23:09:19.628    | 1                |
-- | 189.203.85.229       | www.dataexpert.io           | 2025-02-06 23:10:25.161    | 2                |
-- | 78.135.250.147       | www.dataexpert.io           | 2025-02-06 23:10:21.046    | 2                |
-- | 136.52.6.217         | bootcamp.techcreator.io     | 2025-02-06 23:09:53.718    | 6                |
-- | 71.128.11.123        | bootcamp.techcreator.io     | 2025-02-06 23:08:47.546    | 1                |
-- | 191.156.248.93       | bootcamp.techcreator.io     | 2025-02-06 23:10:18.152    | 2                |
-- | 197.255.164.153      | bootcamp.techcreator.io     | 2025-02-06 23:08:46.053    | 1                |
-- | 187.252.196.233      | www.dataexpert.io           | 2025-02-06 23:11:22.742    | 1                |
-- | 99.149.253.78        | www.dataexpert.io           | 2025-02-06 23:09:15.611    | 1                |
-- | 104.28.55.230        | bootcamp.techcreator.io     | 2025-02-06 23:09:49.267    | 1                |
-- | 52.35.125.236        | bootcamp.techcreator.io     | 2025-02-06 23:10:08.093    | 1                |



SELECT host,
       COUNT(DISTINCT ip) AS unique_users,  -- Get distinct users per host
       CAST(AVG(num_hits) AS INTEGER) AS average_hits_per_host  -- Get avg hits per host
FROM processed_events_ip_aggregated
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')  -- Focus only on required hosts
GROUP BY host
ORDER BY average_hits_per_host DESC;