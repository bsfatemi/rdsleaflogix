# VARS --------------------------------------------------------------------
org <- args$org
org_uuid <- args$uuid
apikey <- args$apikey

# QUERY -------------------------------------------------------------------
organization_js <- hcaonfleet::get_organization(apikey)
delegatees_js <- purrr::map(
  jsonlite::fromJSON(organization_js)$delegatees,
  ~ hcaonfleet::get_delegatee(apikey, .x)
)
jsons <- c(organization_js, unlist(delegatees_js))

# STORE -------------------------------------------------------------------
pipelinetools::storeExternalRaw(jsons, "organizations", org, "onfleet")
