box::use(
  DBI[dbGetQuery],
  hcaconfig[dbc, dbd],
  hcaflowhub[extract_fh_customers, get_customers, get_loyal_customers],
  lubridate[days, today, years],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
apikey <- args$apikey
clientid <- args$clientid

# Query for this dates period.
end_date <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))
loyals_start_date <- end_date - years(5)
orders_start_date <- end_date - days(1)

# `get_loyal_customers` brings customers in bulk; `get_customers` brings customer by customer.
# `get_loyal_customers` brings customers by "created date" (not by updated data), to take advantage
# of bulk query, we query this endpoint for long time before, and orders customers just for short
# period.
# Loyal = opt-ins, if we don't query by orders customers, then we can't notice a customer that
# opted out. That is why we use both approaches. Possibly we should add an alternative to query all
# orders customers in a weekly/monthly pipeline.

# QUERY -------------------------------------------------------------------
loyal_customers_raw <- ""
loyal_customers_ids <- NULL
try(
  {
    loyal_customers_raw <- get_loyal_customers(apikey, clientid, end_date, loyals_start_date)
    loyal_customers_ids <- extract_fh_customers(loyal_customers_raw)$id
  },
  silent = TRUE
)

# Get customers from orders.
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
query_ids <- dbGetQuery(pg, paste0(
  "SELECT DISTINCT customer_id FROM ", paste(org, store, "flowhub_orders ", sep = "_"),
  "WHERE created_at >= '", orders_start_date, "' AND created_at <= '", end_date, "'"
))$customer_id
dbd(pg)
# Don't re-query the ones already obtained.
query_ids <- setdiff(query_ids, loyal_customers_ids)
orders_customers <- get_customers(apikey, clientid, query_ids)

json <- c(loyal_customers_raw, orders_customers)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "flowhub")
