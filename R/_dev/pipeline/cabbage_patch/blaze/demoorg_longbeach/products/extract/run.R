box::use(
  hcablaze[getApiKey],
  hcaconfig[lookupOrgGuid]
)

# VARS --------------------------------------------------------------------
org <- "demoorg"
uuid <- lookupOrgGuid(org)
store <- "longbeach"
apikey <- getApiKey(uuid)$apikey[[store]]

args <- list(org = org, store = store, uuid = uuid, apikey = apikey)

source("inst/pipeline/cabbage_patch/blaze/main/products/extract/run.R", local = TRUE)
