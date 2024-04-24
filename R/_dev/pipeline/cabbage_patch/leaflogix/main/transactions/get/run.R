box::use(
  hcaleaflogix[get_transacts],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
consumerkey <- args$consumerkey
auth <- paste("Basic", args$auth)

# Query for this exact date.
query_date <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
json <- get_transacts(
  query_date, auth, consumerkey, includeDetail = "TRUE", includeTaxes = "TRUE",
  includeOrderIds = "FALSE"
)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "transactions", paste(org, store, sep = "_"), "leaflogix")
