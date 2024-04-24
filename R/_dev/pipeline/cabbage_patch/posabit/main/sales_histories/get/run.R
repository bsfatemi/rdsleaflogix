box::use(
  hcaposabit[get_sales_histories],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
api_token <- args$api_token

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
json <- get_sales_histories(api_token, start_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "sales_histories", paste(org, store, sep = "_"), "posabit")
