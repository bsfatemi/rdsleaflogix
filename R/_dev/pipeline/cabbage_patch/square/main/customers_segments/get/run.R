box::use(
  hcasquare[get_customers_segments],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
access_token <- args$access_token

# QUERY -------------------------------------------------------------------
json <- get_customers_segments(access_token)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers_segments", paste(org, store, sep = "_"), "square")
