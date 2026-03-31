library(DBI)
library(RSQLite)
library(dplyr)

# Connect to database
conn <- dbConnect(SQLite(), "airline2.db")

# Access tables
ontime <- tbl(conn, "ontime")
carriers <- tbl(conn, "carriers")

# Create output folder if needed
if (!dir.exists("r_sql")) {
  dir.create("r_sql")
}

# Step 1: total flights by airline
total_flights <- ontime %>%
  inner_join(carriers, by = c("UniqueCarrier" = "Code")) %>%
  group_by(Description) %>%
  summarise(total_flights = n(), .groups = "drop")

# Step 2: cancelled flights by airline
cancelled_flights <- ontime %>%
  filter(Cancelled == 1) %>%
  inner_join(carriers, by = c("UniqueCarrier" = "Code")) %>%
  group_by(Description) %>%
  summarise(cancelled_flights = n(), .groups = "drop")

# Step 3: compute cancellation rate
result <- total_flights %>%
  inner_join(cancelled_flights, by = "Description") %>%
  mutate(cancellation_rate = cancelled_flights * 1.0 / total_flights) %>%
  arrange(desc(cancellation_rate)) %>%
  head(1) %>%
  collect()

# Show result
print(result)

# Save output
write.csv(result, "r_sql/q4_simplified_output.csv", row.names = FALSE)

# Close connection
dbDisconnect(conn)