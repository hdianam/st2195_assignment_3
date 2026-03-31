#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 31 16:04:02 2026

@author: diana
"""
import os
import sqlite3
import pandas as pd

# ----------------------------
# PATHS - EDIT THESE IF NEEDED
# ----------------------------
db_path = "/Users/diana/Documents/University/Programming/st2195_assignment_3/python_sql/airline2.db"
output_path = "/Users/diana/Documents/University/Programming/st2195_assignment_3/python_sql"


# ----------------------------
# CONNECT TO DATABASE
# ----------------------------
conn = sqlite3.connect(db_path)

# ----------------------------
# QUERY 1
# Which airplane model has the lowest associated average departure delay
# excluding cancelled and diverted flights?
# ----------------------------
q1_sql = """
SELECT
    p.model,
    AVG(o.DepDelay) AS avg_dep_delay
FROM ontime o
JOIN planes p
    ON o.TailNum = p.tailnum
WHERE o.Cancelled = 0
  AND o.Diverted = 0
  AND p.model IS NOT NULL
GROUP BY p.model
ORDER BY avg_dep_delay ASC
LIMIT 1
"""

q1 = pd.read_sql_query(q1_sql, conn)
q1.to_csv(os.path.join(output_path, "q1_output.csv"), index=False)
print("Q1 result:")
print(q1)

# ----------------------------
# QUERY 2
# Which city has the highest number of inbound flights
# excluding cancelled flights?
# ----------------------------
q2_sql = """
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
"""

q2 = pd.read_sql_query(q2_sql, conn)
q2.to_csv(os.path.join(output_path, "q2_output.csv"), index=False)
print("\nQ2 result:")
print(q2)

# ----------------------------
# QUERY 3
# Which company has the highest number of cancelled flights?
# ----------------------------
q3_sql = """
SELECT
    c.Description AS company,
    COUNT(*) AS cancelled_flights
FROM ontime o
JOIN carriers c
    ON o.UniqueCarrier = c.Code
WHERE o.Cancelled = 1
GROUP BY c.Description
ORDER BY cancelled_flights DESC
LIMIT 1
"""

q3 = pd.read_sql_query(q3_sql, conn)
q3.to_csv(os.path.join(output_path, "q3_output.csv"), index=False)
print("\nQ3 result:")
print(q3)

# ----------------------------
# QUERY 4
# Which company has the highest number of cancelled flights,
# relative to their total number of flights?
# ----------------------------
q4_sql = """
SELECT
    c.Description AS company,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN o.Cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_flights,
    CAST(SUM(CASE WHEN o.Cancelled = 1 THEN 1 ELSE 0 END) AS FLOAT) / COUNT(*) AS cancellation_rate
FROM ontime o
JOIN carriers c
    ON o.UniqueCarrier = c.Code
GROUP BY c.Description
ORDER BY cancellation_rate DESC
LIMIT 1
"""

q4 = pd.read_sql_query(q4_sql, conn)
q4.to_csv(os.path.join(output_path, "q4_output.csv"), index=False)
print("\nQ4 result:")
print(q4)


conn.close()

print("\nAll query outputs saved successfully.")