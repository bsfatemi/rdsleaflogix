box::use(
  hcameadow[get_customers],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
consumer_key <- args$consumer_key
client_key <- args$client_key

end_utc <- today("UTC")

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/meadow/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- get_customers(consumer_key, client_key, to = end_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "meadow")
