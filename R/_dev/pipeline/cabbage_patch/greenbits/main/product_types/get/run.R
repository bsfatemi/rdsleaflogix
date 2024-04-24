# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
company_id <- args$company_id
client_id <- args$client_id
token <- args$token

endpt <- "product_types"

# QUERY -------------------------------------------------------------------
json <- hcagreenbits::get_gb(endpt, company_id, client_id, token)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "product_types", paste(org, store, sep = "_"), "greenbits")
