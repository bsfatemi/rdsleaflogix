box::use(
  cli[cli_progress_message],
  DBI[dbGetQuery],
  glue[glue],
  hcaconfig[dbc, dbd],
  hcacova[get_access_token, get_invoice],
  lubridate[days, today],
  pipelinetools[storeExternalRaw]
)

# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
company_id <- args$company_id
client_id <- args$client_id
client_secret <- args$client_secret
username <- args$username
password <- args$password

# Query invoices summaries that were obtained in today's cabbage_patch pipeline.
start_utc <- today("UTC")
end_utc <- today("UTC") + days(1)

# Get invoices IDs to query.
in_table <- paste(org, store, "cova_invoices_summaries", sep = "_")
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
ids <- dbGetQuery(pg, glue("
  SELECT DISTINCT id
  FROM {in_table}
  WHERE '{start_utc}' <= run_date_utc AND run_date_utc < '{end_utc}'
"))$id
dbd(pg)

# QUERY -------------------------------------------------------------------
access_token <- get_access_token(client_id, client_secret, username, password)
json <- lapply(seq_along(ids), function(i) {
  invoice_id <- ids[[i]]
  cli_progress_message("invoice {i}/{length(ids)}")
  get_invoice(access_token, company_id, invoice_id)
})

# STORE -------------------------------------------------------------------
storeExternalRaw(json, "invoices", paste(org, store, sep = "_"), "cova")
