# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
uuid <- args$uuid
apikey <- args$apikey

endpt <- "/store/inventory/brands"

# QUERY -------------------------------------------------------------------
json <- hcablaze::getBlaze(endpt, "start", apikey, org, term = "")

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "brands", paste(org, store, sep = "_"), "blaze")
