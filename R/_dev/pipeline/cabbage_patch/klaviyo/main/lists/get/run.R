# VARS --------------------------------------------------------------------
org <- args$org
uuid <- args$uuid
apikey <- args$apikey

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/klaviyo/", org), showWarnings = FALSE)

# RUN ---------------------------------------------------------------------
json <- hcaklaviyo::get_lists(apikey)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "lists", org, "klaviyo")
