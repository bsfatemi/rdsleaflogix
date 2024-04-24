box::use(
  hcamagento[get_orders],
  lubridate[as_datetime, days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
host <- args$host
access_token <- args$access_token

stop_utc <- as_datetime(today("UTC"))
start_utc <- stop_utc - days(Sys.getenv("PREVIOUS_DAYS", 1))

# QUERY -------------------------------------------------------------------
json <- get_orders(
  host, access_token, start_date = start_utc, end_date = stop_utc
)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "orders", paste(org, sep = "_"), "magento")
