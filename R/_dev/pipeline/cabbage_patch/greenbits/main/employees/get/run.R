# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
company_id <- args$company_id
client_id <- args$client_id
token <- args$token

endpt <- "roles"

# QUERY -------------------------------------------------------------------
json <- hcagreenbits::get_gb(endpt, company_id, client_id, token)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "employees", paste(org, store, sep = "_"), "greenbits")
