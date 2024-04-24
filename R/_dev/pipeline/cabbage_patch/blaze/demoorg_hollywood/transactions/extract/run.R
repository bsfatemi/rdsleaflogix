box::use(
  hcablaze[getApiKey],
  hcaconfig[lookupOrgGuid]
)

# VARS --------------------------------------------------------------------
org <- "demoorg"
uuid <- lookupOrgGuid(org)
store <- "hollywood"
apikey <- getApiKey(uuid)$apikey[[store]]

args <- list(org = org, store = store, uuid = uuid, apikey = apikey)

source("inst/pipeline/cabbage_patch/blaze/main/transactions/extract/run.R", local = TRUE)
