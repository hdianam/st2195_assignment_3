library(DBI)
library(RSQLite)

db_path <- "/Users/diana/Documents/University/Programming/r_sql/airline2.db"

conn <- dbConnect(SQLite(), db_path)

print(dbListTables(conn))

# Q1: Which of the following airplanes has the lowest associated average departure delay (excluding cancelled and diverted flights)?
q1 <- dbGetQuery(conn, "
SELECT
    p.model,
    AVG(o.DepDelay) AS avg_dep_delay
FROM ontime o
JOIN planes p
    ON o.TailNum = p.tailnum
WHERE o.Cancelled = 0
  AND o.Diverted = 0
  AND o.DepDelay IS NOT NULL
  AND p.model IS NOT NULL
GROUP BY p.model
ORDER BY avg_dep_delay ASC
LIMIT 1
")
print(q1)

# Q2: Which of the following cities has the highest number of inbound flights (excluding cancelled flights)?
q2 <- dbGetQuery(conn, "
SELECT
    a.city,
    COUNT(*) AS inbound_flights
FROM ontime o
JOIN airports a
    ON o.Dest = a.iata
WHERE o.Cancelled = 0
GROUP BY a.city
ORDER BY inbound_flights DESC
LIMIT 1
")
print(q2)

# Q3: company with highest number of cancelled flights
q3 <- dbGetQuery(conn, "
SELECT
    c.Description AS carrier,
    COUNT(*) AS cancelled_flights
FROM ontime o
JOIN carriers c
    ON o.UniqueCarrier = c.Code
WHERE o.Cancelled = 1
GROUP BY c.Description
ORDER BY cancelled_flights DESC
LIMIT 1
")
print(q3)

# Q4: company with highest cancelled flights relative to total flights
q4 <- dbGetQuery(conn, "
SELECT
    c.Description AS carrier,
    COUNT(CASE WHEN o.Cancelled = 1 THEN 1 END) * 1.0 / COUNT(*) AS cancellation_ratio
FROM ontime o
JOIN carriers c
    ON o.UniqueCarrier = c.Code
GROUP BY c.Description
ORDER BY cancellation_ratio DESC
LIMIT 1
")
print(q4)

# ---- save outputs ----
output_folder <- "/Users/diana/Documents/University/Programming/r_sql"

write.csv(q1, file.path(output_folder, "q1_output.csv"), row.names = FALSE)
write.csv(q2, file.path(output_folder, "q2_output.csv"), row.names = FALSE)
write.csv(q3, file.path(output_folder, "q3_output.csv"), row.names = FALSE)
write.csv(q4, file.path(output_folder, "q4_output.csv"), row.names = FALSE)

# ---- disconnect ----
dbDisconnect(conn)