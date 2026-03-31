library(DBI)
library(RSQLite)
library(readr)

data_path <- "/Users/diana/Downloads/dataverse_files"
if (file.exists("airline.db")) {
  file.remove("airline2.db")
}
conn <- dbConnect(SQLite(), "airline2.db")

airports <- read.csv(file.path(data_path, "airports.csv"))
carriers <- read.csv(file.path(data_path, "carriers.csv"))
planes <- read.csv(file.path(data_path, "plane-data.csv"))

dbWriteTable(conn, "airports", airports, overwrite = TRUE)
dbWriteTable(conn, "carriers", carriers, overwrite = TRUE)
dbWriteTable(conn, "planes", planes, overwrite = TRUE)

ontime_2000 <- read_csv(file.path(data_path, "2000.csv"))
dbWriteTable(conn, "ontime", ontime_2000, overwrite = TRUE)
for (yr in 2001:2005) {
  yearly_data <- read_csv(file.path(data_path, paste0(yr, ".csv")))
  dbWriteTable(conn, "ontime", yearly_data, append = TRUE)
}
print(dbListTables(conn))
dbDisconnect(conn)

