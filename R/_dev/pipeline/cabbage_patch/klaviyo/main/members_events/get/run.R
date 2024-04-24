box::use(
  hcaconfig[dbc, dbd],
  hcaklaviyo[get_profile_events_for_all_metrics],
  cli[cli_progress_message],
  lubridate[days, today],
  pipelinetools[db_read_table_unique, storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
uuid <- args$uuid
apikey <- args$apikey
since_utc <- today() - days(Sys.getenv("PREVIOUS_DAYS", 1))

# GET ---------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
members <- db_read_table_unique(
  pg, paste(org, "klaviyo_members", sep = "_"), "id", "run_date_utc"
)
dbd(pg)

# RUN ---------------------------------------------------------------------
ids_to_query <- unique(members$id)
json <- lapply(seq_along(ids_to_query), function(i) {
  person_id <- ids_to_query[[i]]
  cli_progress_message("{i} / {length(ids_to_query)}")
  get_profile_events_for_all_metrics(person_id, apikey, since_utc)
})

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "members_events", org, "klaviyo")
