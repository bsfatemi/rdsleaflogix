box::use(
  hcameadow[get_products],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
consumer_key <- args$consumer_key
client_key <- args$client_key

# QUERY -------------------------------------------------------------------
json <- get_products(consumer_key, client_key)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "products", paste(org, store, sep = "_"), "meadow")
