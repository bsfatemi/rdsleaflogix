# VARS --------------------------------------------------------------------
org <- args$org
host <- args$host
access_token <- args$access_token

# Create raw data storage dir if it does not exist.
dir.create(paste0("/mnt/data/pipeline/raw/magento/", org), showWarnings = FALSE)

# QUERY -------------------------------------------------------------------
json <- hcamagento::get_brands(host, access_token)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "brands", paste(org, sep = "_"), "magento")
