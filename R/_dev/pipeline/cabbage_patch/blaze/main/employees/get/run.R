# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
apikey <- args$apikey

endpt <- "/employees"

# QUERY -------------------------------------------------------------------
json <- hcablaze::getBlaze(endpt, "start", apikey, org)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "employees", paste(org, store, sep = "_"), "blaze")
