box::use(
  hcasquare[get_customers],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
access_token <- args$access_token

end_utc <- today("UTC")
start_utc <- end_utc - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
json <- get_customers(access_token, start_utc, end_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "square")
