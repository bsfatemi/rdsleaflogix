box::use(
  DBI[dbGetQuery],
  glue[glue],
  hcabudee[get_deliveries_details],
  hcaconfig[dbc, dbd],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
apikey <- args$apikey
in_table <- paste(org, store, "budee_deliveries", sep = "_")
out_table <- paste(org, store, "budee_deliveries_details", sep = "_")

# INPUTS ------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
# Pull those `deliveries_details` that haven't been pulled already.
deliveries_ids <- dbGetQuery(pg, glue("
  SELECT DISTINCT id
  FROM {in_table}
  WHERE NOT EXISTS (
    SELECT 1 FROM {out_table}
    WHERE ({in_table}.id = {out_table}.id)
  )
"))$id
dbd(pg)

# QUERY -------------------------------------------------------------------
json <- get_deliveries_details(apikey, deliveries_ids)

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "deliveries_details", paste(org, store, sep = "_"), "budee")
