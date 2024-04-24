box::use(
  hcaseedtalent[get_access_token, get_courses],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
integrator_id <- args$integrator_id
api_key <- args$api_key

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/seedtalent/all_", integrator_id), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
access_token <- get_access_token(integrator_id, api_key)
json <- get_courses(access_token)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "courses", paste("all", integrator_id, sep = "_"), "seedtalent")
