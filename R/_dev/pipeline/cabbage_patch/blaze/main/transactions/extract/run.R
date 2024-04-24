# VARS --------------------------------------------------------------------
org <- args$org
store <- args$store
out_table <- paste(org, store, "blaze_transactions", sep = "_")
out_table_ci <- paste(org, store, "blaze_transactions_cart_items", sep = "_")

# READ --------------------------------------------------------------------
json <- pipelinetools::rd_raw_archive("transactions", paste(org, store, sep = "_"), "blaze")

# EXTRACT -----------------------------------------------------------------
transactions <- hcablaze::extractBlaze(json, "transactions")
run_utc <- lubridate::now(tzone = "UTC")
cart_items <- transactions$transactions_cart_items
transactions <- transactions$transactions
cart_items$run_date_utc <- run_utc
transactions$run_date_utc <- run_utc

# WRITE -------------------------------------------------------------------
pg <- hcaconfig::dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
transactions <- pipelinetools::check_cols(transactions, DBI::dbListFields(pg, out_table))
DBI::dbWriteTable(pg, out_table, transactions, append = TRUE)
cart_items <- pipelinetools::check_cols(cart_items, DBI::dbListFields(pg, out_table_ci))
DBI::dbWriteTable(pg, out_table_ci, cart_items, append = TRUE)
hcaconfig::dbd(pg)
