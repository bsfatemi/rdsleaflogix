# Integrated run.R
#
# Purge the box module cache, so the app can be reloaded without restarting the R session.
rm(list = ls(box:::loaded_mods), envir = box:::loaded_mods)

# Allow absolute module imports (relative to the project root).
options(box.path = getwd())

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbWriteTable],
  dplyr[coalesce, mutate],
  hcaconfig[dbc, dbd, orgIndex],
  hcapipelines[plIndex, get_pipeline_child_process_count],
  jsonlite[fromJSON, toJSON],
  logger[log_info, log_success],
  parallel[mclapply]
)

# Source ----------------------------------------------------------------------
source("inst/pipeline/integrated/population/src/build_population.R")
# Read conf -------------------------------------------------------------------
orgs_uuids <- orgIndex()[in_pop == TRUE]
# Calculate population just for orgs `in_pop` & `curr_client`.
filtered_orgs <- plIndex()[org_uuid %in% orgs_uuids[curr_client == TRUE, unique(org_uuid)], .SD]
tbls <- fromJSON(toJSON(filtered_orgs), simplifyDataFrame = FALSE)

# Build and write data --------------------------------------------------------
log_info("Population build for all orgs starting.")
population_res <- mclapply(tbls, function(org) {
  # Don't fail if couldn't fully execute.
  try({
    log_info("Building population for:", org$short_name)
    pg_in <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
    org_population <- build_population(pg_in, org)
    dbd(pg_in)

    log_info("Deleting old population data for:", org$short_name)
    pg_out <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
    dbBegin(pg_out)
    dbExecute(pg_out, paste0("DELETE FROM population WHERE org_uuid = '", org$org_uuid, "'"))

    dbWriteTable(pg_out, "population", org_population, append = TRUE)
    dbCommit(pg_out)
    dbd(pg_out)
    log_info(paste("Wrote new population data for:", org$short_name))
  })
}, mc.cores = get_pipeline_child_process_count())

pg_out <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
log_info("Delete rows for all inactive orgs.")
dbExecute(pg_out, paste0(
  "DELETE FROM population WHERE org_uuid NOT IN ('",
  paste(orgs_uuids$org_uuid, collapse = "','"),
  "')"
))
dbd(pg_out)

# Notify errors ---------------------------------------------------------------
log_info("Reporting orgs that errored.")
errored_orgs <- sapply(tbls, function(x) x$short_name)[
  sapply(population_res, function(x) inherits(x, "try-error"))
]
if (length(errored_orgs) > 0) {
  stop("Population build failed for: ", paste(errored_orgs, collapse = ", "))
}

log_success("Population build for all orgs finished.")
