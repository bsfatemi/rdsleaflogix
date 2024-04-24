# VARS --------------------------------------------------------------------
org <- args$org
org_uuid <- args$uuid
apikey <- args$apikey

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/onfleet/", org), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- hcaonfleet::get_workers(apikey)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "workers", org, "onfleet")
