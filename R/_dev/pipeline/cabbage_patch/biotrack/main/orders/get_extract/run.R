box::use(
  DBI[dbListFields, dbWriteTable],
  dplyr[mutate],
  hcabiotrack[get_orders],
  hcaconfig[dbc, dbd],
  jsonlite[fromJSON],
  lubridate[days, now, today],
  pipelinetools[check_cols]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "biotrack_orders", sep = "_")
host <- args$host
port <- args$port
dbname <- args$dbname
username <- args$username
password <- args$password
locations_ids <- args$locations_ids

# The DB returns it as NA, while it should be NULL.
if (is.na(locations_ids)) {
  locations_ids <- NULL
}
if (!is.null(locations_ids)) {
  locations_ids <- fromJSON(locations_ids)
}

end_utc <- today("UTC")
start_utc <- end_utc - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
orders <- get_orders(
  host, port, dbname, username, password, start_utc, end_utc, locations_ids
) |>
  mutate(run_date_utc = now("UTC"))

# WRITE -------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
orders <- check_cols(orders, dbListFields(pg, out_table))
dbWriteTable(pg, out_table, orders, append = TRUE)
dbd(pg)
