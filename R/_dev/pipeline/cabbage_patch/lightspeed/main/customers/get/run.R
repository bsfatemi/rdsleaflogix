box::use(
  hcalightspeed[get_access_token, get_account, get_customers],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
refresh_token <- args$refresh_token

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/lightspeed/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
access_token <- get_access_token(refresh_token)
account_id <- get_account(access_token)$Account$accountID
json <- get_customers(account_id, access_token, refresh_token, start_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "customers", paste(org, store, sep = "_"), "lightspeed")
