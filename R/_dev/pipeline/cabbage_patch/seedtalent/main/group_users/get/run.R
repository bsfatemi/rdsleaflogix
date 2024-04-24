box::use(
  hcaconfig[dbc, dbd],
  hcaseedtalent[get_access_token, get_group_users],
  pipelinetools[db_read_table_unique, storeExternalRaw],
  purrr[map]
)

# VARS --------------------------------------------------------------------
short_name <- args$short_name
integrator_id <- args$integrator_id
api_key <- args$api_key
organization_id <- args$organization_id

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/seedtalent/", short_name), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
# Get org groups.
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
groups_ids <- db_read_table_unique(
  pg, "seedtalent_groups", "id", "run_date_utc", columns = "id",
  extra_filters = paste0("organization_id = '", organization_id, "'")
)$id
dbd(pg)
# Query.
access_token <- get_access_token(integrator_id, api_key)
jsons <- map(groups_ids, function(group_id) {
  get_group_users(group_id, access_token)
})

# STORE -------------------------------------------------------------------
storeExternalRaw(jsons, "group_users", short_name, "seedtalent")
