# VARS --------------------------------------------------------------------
org <- args$org
uuid <- args$uuid
apikey <- args$apikey

# GET ---------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
members <- pipelinetools::db_read_table_unique(
  pg, paste(org, "klaviyo_members", sep = "_"), "id", "run_date_utc"
)
hcaconfig::dbd(pg)

# RUN ---------------------------------------------------------------------
ids_to_query <- unique(members$id)
json <- lapply(seq_along(ids_to_query), function(i) {
  person_id <- ids_to_query[[i]]
  cli::cli_progress_message("{i} / {length(ids_to_query)}")
  hcaklaviyo::get_profile(person_id, apikey = apikey)
})

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "members_profiles", org, "klaviyo")
