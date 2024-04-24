box::use(
  hcaonfleet[get_tasks],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
org_uuid <- args$uuid
apikey <- args$apikey

# Query for these date ranges.
start_utc <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))
end_utc <- today("UTC")

# QUERY -------------------------------------------------------------------
json <- get_tasks(apikey, start_utc, end_utc)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "tasks", org, "onfleet")
