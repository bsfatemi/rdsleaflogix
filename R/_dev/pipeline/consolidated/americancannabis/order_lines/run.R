# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  hcaconfig[dbc, dbd],
  hcaproteus[build_proteus_order_lines],
  DBI[dbBegin, dbCommit, dbExecute, dbReadTable, dbWriteTable],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  dplyr[select]
)

### VARS
org <- "americancannabis"
org_uuid <- "b19a6ecc-4137-4190-aa2e-c5dd8ad27b3a"

### READ FROM CABBAGE_PATCH
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
price_types <- dbReadTable(pg, "proteus_price_types")
invoice_line_items <- db_read_table_unique(
  pg, paste0(org, "_proteus_invoice_line_items"), c("invoice_id", "product_id"), "run_date_utc",
  with_ties = TRUE
)
products <- db_read_table_unique(pg, paste0(org, "_proteus_products"), "product_id", "dt_modified")
discs_taxes <- db_read_table_unique(
  pg, paste0(org, "_proteus_invoices"), "invoice_id", "run_date_utc",
  columns = c(
    "invoice_id", "customer_id", "status", "discountamt", "addtax",
    "run_date_utc"
  )
)
dbd(pg)

### READ FROM CONSOLIDATED
cats_map <- select(get_categories_mapping(org_uuid), -run_date_utc)

### BUILD
order_lines <- build_proteus_order_lines(
  org, invoice_line_items, products, price_types, discs_taxes, cats_map
)

### WRITE
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_order_lines")))
dbWriteTable(pg, paste0(org, "_order_lines"), order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
