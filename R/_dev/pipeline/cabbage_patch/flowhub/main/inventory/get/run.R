box::use(
  hcaflowhub[get_products],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
apikey <- args$apikey
clientid <- args$clientid

# QUERY -------------------------------------------------------------------
json <- get_products(apikey, clientid)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "products", paste(org, store, sep = "_"), "flowhub")
