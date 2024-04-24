# Clair appdata - customer profile - run.R
#

box::use(
  DBI[dbAppendTable, dbBegin, dbCommit, dbExecute, Id],
  hcaconfig[dbc, dbd, orgIndex],
  hcapipelines[plIndex],
  parallel[mclapply],
  purrr[map_lgl, transpose]
)

# Source ----------------------------------------------------------------------
source("inst/pipeline/appdata/clair/customer_profile/src/build_customer_profile.R")

# Read conf -------------------------------------------------------------------
orgs_uuids <- orgIndex()[curr_client == TRUE]
# Calculate customer_profile just for orgs `curr_client`.
filtered_orgs <- plIndex()[org_uuid %in% orgs_uuids$org_uuid]

# Build and write data --------------------------------------------------------
successes <- mclapply(transpose(filtered_orgs), function(org) {
  # Don't fail if couldn't fully execute.
  try({
    # Build the customers profile for one org.
    pg_in <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
    org_pop <- read_org_pop(pg_in, org$org_uuid)
    dbd(pg_in)
    customer_profile <- build_customer_profile(org_pop)
    rm(org_pop)
    # Delete the customers profile data for the org (once it is already built).
    pg_out <- dbc(Sys.getenv("HCA_ENV", "prod2"), "appdata")
    dbBegin(pg_out)
    dbExecute(pg_out, paste0(
      "DELETE FROM clair.customer_profile WHERE org_uuid = '", org$org_uuid, "'"
    ))
    # Write the org's customers profile to database.
    dbAppendTable(pg_out, Id(schema = "clair", table = "customer_profile"), customer_profile)
    dbCommit(pg_out)
    dbd(pg_out)
  })
}, mc.cores = 4L)

# Database maintenance for customer_profile -----------------------------------
pg_out <- dbc(Sys.getenv("HCA_ENV", "prod2"), "appdata")
# Delete rows for inactive orgs.
dbExecute(pg_out, paste0(
  "DELETE FROM clair.customer_profile WHERE org_uuid NOT IN ('",
  paste(filtered_orgs$org_uuid, collapse = "','"),
  "')"
))
dbExecute(pg_out, "VACUUM ANALYZE clair.customer_profile")
dbd(pg_out)

# Notify errors ---------------------------------------------------------------
errored_orgs <- filtered_orgs$short_name[map_lgl(successes, ~ inherits(.x, "try-error"))]
if (length(errored_orgs) > 0) {
  stop("customer_profile build failed for: ", paste(errored_orgs, collapse = ", "))
}
