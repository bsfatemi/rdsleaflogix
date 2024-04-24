box::use(
  hcawebjoint[getWJToken, get_customers],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
domain <- args$domain
email <- args$email
password <- args$password
token <- args$token
facility_id <- args$facility_id

# Query since this date (but will contain some rows with dates from before).
start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/webjoint/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- get_customers(
  list(domain = domain, email = email, pw = password), facility_id, start_utc, token
)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "webjoint")
