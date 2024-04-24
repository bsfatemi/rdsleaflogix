# VARS --------------------------------------------------------------------
org <- args$org
host <- args$host
access_token <- args$access_token

# QUERY -------------------------------------------------------------------
json <- hcamagento::get_categories(host, access_token)

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(json, "categories", paste(org, sep = "_"), "magento")
