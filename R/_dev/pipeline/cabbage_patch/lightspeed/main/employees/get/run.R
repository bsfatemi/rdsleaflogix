box::use(
  hcalightspeed[get_access_token, get_account, get_employees],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
refresh_token <- args$refresh_token

start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
access_token <- get_access_token(refresh_token)
account_id <- get_account(access_token)$Account$accountID
json <- get_employees(account_id, access_token, refresh_token, start_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "employees", paste(org, store, sep = "_"), "lightspeed")
