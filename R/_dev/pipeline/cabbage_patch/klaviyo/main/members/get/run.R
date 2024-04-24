# VARS --------------------------------------------------------------------
org <- args$org
uuid <- args$uuid
apikey <- args$apikey

# GET ---------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
lists <- pipelinetools::db_read_table_unique(
  pg, paste(org, "klaviyo_lists", sep = "_"), "id", "run_date_utc"
)
hcaconfig::dbd(pg)

# RUN ---------------------------------------------------------------------
json <- lapply(lists$id, hcaklaviyo::get_list_and_segment_members, apikey = apikey)
names(json) <- lists$id

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "members", org, "klaviyo")
