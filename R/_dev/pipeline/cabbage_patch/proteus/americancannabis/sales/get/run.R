box::use(
  pipelinetools[storeExternalRaw],
  hcaconfig[lookupOrgGuid],
  hcaproteus[get_proteus_sales, get_api_keys]
)

# vars --------------------------------------------------------------------
org <- "americancannabis"
orguuid <- lookupOrgGuid(org)
apikey <- get_api_keys(orguuid)

# query -------------------------------------------------------------------

js <- get_proteus_sales(apikey)

# write -------------------------------------------------------------------

storeExternalRaw(js, "sales", org, "proteus")
