box::use(
  hcaseedtalent[get_access_token, get_groups],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
integrator_id <- args$integrator_id
api_key <- args$api_key

# QUERY -------------------------------------------------------------------
access_token <- get_access_token(integrator_id, api_key)
json <- get_groups(access_token)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "groups", paste("all", integrator_id, sep = "_"), "seedtalent")
