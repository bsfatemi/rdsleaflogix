box::use(
  hcaleaflogix[get_loyalty],
  pipelinetools[storeExternalRaw]
)

# VARS ------------------------------------------------------------------------
org <- args$org
store <- args$store
consumerkey <- args$consumerkey
auth <- paste("Basic", args$auth)

# QUERY -----------------------------------------------------------------------
json <- get_loyalty(auth, consumerkey)

# ARCHIVE ---------------------------------------------------------------------
storeExternalRaw(json, "loyalty", paste(org, store, sep = "_"), "leaflogix")
