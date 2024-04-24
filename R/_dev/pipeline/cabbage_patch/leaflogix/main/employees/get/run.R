box::use(
  hcaleaflogix[get_employees],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
consumerkey <- args$consumerkey
auth <- paste("Basic", args$auth)

# QUERY -------------------------------------------------------------------
json <- get_employees(auth, consumerkey)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "employees", paste(org, store, sep = "_"), "leaflogix")
