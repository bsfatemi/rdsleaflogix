box::use(
  hcawebjoint[getWJToken, get_deliveryzones],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
domain <- args$domain
email <- args$email
password <- args$password
token <- args$token
facility_id <- args$facility_id

# QUERY -------------------------------------------------------------------
json <- get_deliveryzones(list(domain = domain, email = email, pw = password), facility_id, token)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "deliveryzones", paste(org, store, sep = "_"), "webjoint")
