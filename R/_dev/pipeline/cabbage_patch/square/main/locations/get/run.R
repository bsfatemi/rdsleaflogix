box::use(
  hcasquare[get_locations],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
access_token <- args$access_token

# QUERY -------------------------------------------------------------------
json <- get_locations(access_token)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "locations", paste(org, store, sep = "_"), "square")
