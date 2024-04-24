box::use(
  hcablaze[getApiKey],
  hcaconfig[lookupOrgGuid]
)

# VARS --------------------------------------------------------------------
org <- "demoorg"
uuid <- lookupOrgGuid(org)
store <- "noho"
apikey <- getApiKey(uuid)$apikey[[store]]

args <- list(org = org, store = store, uuid = uuid, apikey = apikey)

source("inst/pipeline/cabbage_patch/blaze/main/employees/get/run.R", local = TRUE)
