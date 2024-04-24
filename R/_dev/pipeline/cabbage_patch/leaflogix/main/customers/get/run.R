box::use(
  hcaleaflogix[get_customers],
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

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/leaflogix/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- get_customers(query_date, auth, consumerkey, includeAnonymous = "TRUE")

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "leaflogix")
