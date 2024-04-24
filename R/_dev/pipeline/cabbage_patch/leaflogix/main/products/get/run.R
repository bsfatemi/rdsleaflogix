box::use(
  hcaleaflogix[get_products],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
consumerkey <- args$consumerkey
auth <- paste("Basic", args$auth)

eob_utc <- today() - days(Sys.getenv("PREVIOUS_DAYS", 1))
# QUERY -------------------------------------------------------------------
json <- get_products(auth, consumerkey, fromLastModifiedDateUTC = eob_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "products", paste(org, store, sep = "_"), "leaflogix")
