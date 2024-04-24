# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
company_id <- args$company_id
client_id <- args$client_id
token <- args$token

endpt <- "inventory_items"

# QUERY -------------------------------------------------------------------
json <- hcagreenbits::get_gb(endpt, company_id, client_id, token)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "inventory_items", paste(org, store, sep = "_"), "greenbits")
