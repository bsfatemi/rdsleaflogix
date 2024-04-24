box::use(
  hcasquare[get_loyalty_accounts],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
access_token <- args$access_token

# QUERY -------------------------------------------------------------------
json <- get_loyalty_accounts(access_token)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "loyalty_accounts", paste(org, store, sep = "_"), "square")
