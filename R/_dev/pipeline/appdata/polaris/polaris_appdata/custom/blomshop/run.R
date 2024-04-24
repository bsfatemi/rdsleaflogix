# Marketing Studio App Data
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  DBI[dbGetQuery, dbListTables, dbWriteTable, dbReadTable, dbBegin, dbExecute, dbCommit],
  dplyr[as_tibble, filter, pull, rename],
  hcaconfig[dbc, dbd],
  hcapipelines[plAppDataIndex],
  lubridate[now],
  pipelinetools[db_read_table_unique],
  purrr[map_dfr],
  stringr[str_detect, str_glue]
)

# SOURCE ------------------------------------------------------------------
source(
  "inst/pipeline/appdata/polaris/polaris_appdata/custom/blomshop/src/build_blomshop_polaris.R",
  local = TRUE
)

# POLARIS-APPDATA READ ---------------------------------------------------------------

# ONLY RUN FOR SELECTED ORGS
orgQuery <- "blomshop"

## Adding abandoned carts
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
# Grab all active stores
active_ihj_stores <- as_tibble(dbListTables(pg)) |>
  filter(str_detect(string = value, pattern = "_iheartjane_reservations$")) |>
  pull(value)
abandoned_carts <- map_dfr(active_ihj_stores, function(store) {
  db_read_table_unique(
    pg, store, "id", "run_date_utc",
    columns = c(
      "id", "customer_phone_number", "customer_email_address", "created_at", "orguuid", "status"
    )
  ) |>
    rename(phone = customer_phone_number, email = customer_email_address) |>
    filter(status == "pending")
})

blomshop_mapping <- db_read_table_unique(
  pg, "blomshop_main_square_loyalty_accounts", "id", "run_date_utc",
  columns = c("customer_id", "run_date_utc")
)
dbd(pg)

# connect and read
cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
customer_behavior <- dbGetQuery(
  cn, str_glue("SELECT * FROM customer_behavior WHERE org IN ('{orgQuery}')")
)
customers <- dbGetQuery(
  cn, str_glue("SELECT * FROM customers WHERE org IN ('{orgQuery}')")
)
dbd(cn)

#  POLARIS-APPDATA ---------------------------------------------------------------
polaris <- build_blomshop_polaris(customers, customer_behavior, abandoned_carts, blomshop_mapping)
polaris$run_date_utc <- now(tzone = "UTC")

# THESE ORGS WANT CUSTOMERS WHO HAVE NEVER ORDERED REMOVE FROM THEIR DATA
only_order_orgs <- plAppDataIndex()[
  polaris_is_active & !polaris_incl_no_orders & !polaris_manual_pl, unique(short_name)
]
polaris <- filter(polaris, !(num_orders == 0 & org %in% only_order_orgs))

# WRITE ---------------------------------------------------------------
orgQuery <- paste(unique(polaris$org), collapse = "', '")
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "appdata")
dbBegin(pg)
dbExecute(pg, str_glue("DELETE FROM polaris_appdata WHERE org IN ('{orgQuery}')"))
dbWriteTable(pg, "polaris_appdata", polaris, append = TRUE)
dbCommit(pg)
dbd(pg)
