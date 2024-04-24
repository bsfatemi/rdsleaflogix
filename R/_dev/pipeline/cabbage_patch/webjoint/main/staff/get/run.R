box::use(
  hcawebjoint[getWJToken, get_staff],
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

# QUERY -------------------------------------------------------------------
json <- get_staff(
  list(domain = domain, email = email, pw = password), facility_id, start_utc, token
)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "staff", paste(org, store, sep = "_"), "webjoint")
