library(DBI)
library(RSQLite)
library(dplyr)

conn <- dbConnect(SQLite(), "airline2.db")

ontime   <- tbl(conn, "ontime")
airports <- tbl(conn, "airports")
carriers <- tbl(conn, "carriers")
planes   <- tbl(conn, "planes")

# Q1
q1 <- ontime %>%
  filter(Cancelled == 0, Diverted == 0) %>%
  inner_join(planes, by = c("TailNum" = "tailnum")) %>%
  filter(!is.na(model)) %>%
  group_by(model) %>%
  summarise(avg_dep_delay = mean(DepDelay, na.rm = TRUE)) %>%
  arrange(avg_dep_delay) %>%
  collect() %>%
  slice(1)

write.csv(q1, "q1_output.csv", row.names = FALSE)

# Q2
q2 <- ontime %>%
  filter(Cancelled == 0) %>%
  inner_join(airports, by = c("Dest" = "iata")) %>%
  group_by(city) %>%
  summarise(inbound_flights = n()) %>%
  arrange(desc(inbound_flights)) %>%
  collect() %>%
  slice(1)

write.csv(q2, "q2_output.csv", row.names = FALSE)

# Q3
q3 <- ontime %>%
  filter(Cancelled == 1) %>%
  inner_join(carriers, by = c("UniqueCarrier" = "Code")) %>%
  group_by(Description) %>%
  summarise(cancelled_flights = n()) %>%
  arrange(desc(cancelled_flights)) %>%
  collect() %>%
  slice(1)

write.csv(q3, "q3_output.csv", row.names = FALSE)

# Q4
q4 <- ontime %>%
  inner_join(carriers, by = c("UniqueCarrier" = "Code")) %>%
  group_by(Description) %>%
  summarise(
    total_flights = n(),
    cancelled_flights = sum(Cancelled, na.rm = TRUE)
  ) %>%
  mutate(cancellation_rate = cancelled_flights / total_flights) %>%
  arrange(desc(cancellation_rate)) %>%
  collect() %>%
  slice(1)

write.csv(q4, "q4_output.csv", row.names = FALSE)

dbDisconnect(conn)