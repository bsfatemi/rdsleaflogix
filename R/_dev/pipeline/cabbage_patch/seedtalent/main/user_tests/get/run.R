box::use(
  hcaconfig[dbc, dbd],
  hcaseedtalent[get_access_token, get_user_tests],
  pipelinetools[db_read_table_unique, storeExternalRaw],
  purrr[map]
)

# VARS --------------------------------------------------------------------
short_name <- args$short_name
integrator_id <- args$integrator_id
api_key <- args$api_key

# QUERY -------------------------------------------------------------------
# Get org users.
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
users_ids <- db_read_table_unique(
  pg, paste0(short_name, "_seedtalent_users"), "id", "run_date_utc", columns = "id"
)$id
dbd(pg)
# Query.
access_token <- get_access_token(integrator_id, api_key)
jsons <- map(users_ids, function(user_id) {
  get_user_tests(user_id, access_token)
})

# STORE -------------------------------------------------------------------
storeExternalRaw(jsons, "user_tests", short_name, "seedtalent")
