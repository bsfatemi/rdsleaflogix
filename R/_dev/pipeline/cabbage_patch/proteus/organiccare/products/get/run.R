box::use(
  pipelinetools[storeExternalRaw],
  hcaconfig[lookupOrgGuid],
  hcaproteus[get_proteus_products, get_api_keys]
)

# vars --------------------------------------------------------------------
org <- "organiccare"
orguuid <- lookupOrgGuid(org)
apikey <- get_api_keys(orguuid)

# query -------------------------------------------------------------------

js <- get_proteus_products(apikey)

# write -------------------------------------------------------------------

storeExternalRaw(js, "products", org, "proteus")
