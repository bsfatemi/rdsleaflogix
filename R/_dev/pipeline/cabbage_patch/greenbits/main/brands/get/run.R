# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
company_id <- args$company_id
client_id <- args$client_id
token <- args$token

endpt <- "brands"

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/greenbits/", org, "_", store), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- hcagreenbits::get_gb(endpt, company_id, client_id, token)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "brands", paste(org, store, sep = "_"), "greenbits")
