###
###
### NOTE:
###
### This needs to run every hour 8am-8pm
###
### TODO:
###
### Dawnstar data foundation starts here. This script is for blaze.
### The equivalent needs to be run for Webjoint and Treez. Then the
### next stage of this pipeline is to take the output of all three
### and build data for the population.stock.snapshots schema
###
### CRITICAL:
###   This data results in brandId's given from blaze. However, there is no
###   additional brand info. This script needs an additional call to blazeapi
###   to retrieve the brand name associated with the blaze brandIds we have.
###
###
###
###

# load import packages ----------------------------------------------------
box::use(
  data.table[as.data.table],
  hcablaze[get_api_keys],
  hcaconfig[get_org_stores],
)

# utility functions -------------------------------------------------------
keyIndex <- function(org_index) {
  creds <- read_yaml(hcablaze:::..xdfp("src-blaze.yml"))[[1]]$creds$clients
  ORGS <- rbindlist(
    lapply(creds, function(org_creds) { # For each org_uuid.
      org_uuid <- names(org_creds)
      org_creds <- org_creds[[1]]
      # Unnamed stores case.
      if (is.null(names(org_creds))) {
        org_creds <- list(main = org_creds)
      }
      # Sort stores alphabetically by name.
      org_creds <- org_creds[order(names(org_creds))]
      org_creds <- bind_rows(lapply(names(org_creds), function(facility) {
        data.frame(org_uuid = org_uuid, facility = facility, apikey = org_creds[[facility]])
      }))
      org_creds$org <- filter(org_index, org_uuid == !!org_uuid)$short_name
      org_creds
    })
  ) |>
    # Add `store_uuid` by joining by `facility`.
    left_join(
      select(get_org_stores(), org_uuid, facility, store_uuid),
      by = c("org_uuid", "facility")
    )
  db_creds <- select(
    get_api_keys(), org_uuid, org = org_short_name, store_uuid, facility = store_short_name, apikey
  )
  bind_rows(ORGS, db_creds)
}
