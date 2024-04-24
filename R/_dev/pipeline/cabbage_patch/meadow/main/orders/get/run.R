box::use(
  hcameadow[get_orders],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
consumer_key <- args$consumer_key
client_key <- args$client_key

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))
end_utc <- today("UTC")

# QUERY -------------------------------------------------------------------
json <- get_orders(consumer_key, client_key, start_utc, end_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "orders", paste(org, store, sep = "_"), "meadow")
