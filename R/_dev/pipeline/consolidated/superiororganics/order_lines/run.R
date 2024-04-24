# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, lookupOrgGuid],
  hcatreez[build_trz2_order_lines],
  lubridate[now],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  dplyr[select]
)

### VARS
org <- "superiororganics"
store <- "main"
org_uuid <- lookupOrgGuid(org)

### READ FROM CABBAGE_PATCH
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
treez_tickets <- db_read_table_unique(
  pg, paste(org, store, "trz2_tickets", sep = "_"), "order_number", "run_date_utc",
  columns = c("order_number", "run_date_utc", "customer_id", "ticket_id")
)
treez_ticket_items <- db_read_table_unique(
  pg, paste(org, store, "trz2_ticket_items", sep = "_"), "ticket_id", "run_date_utc",
  with_ties = TRUE,
  columns = c("ticket_id", "product_id", "run_date_utc", "quantity", "price_sell", "price_total")
)
treez_ticket_discounts <- db_read_table_unique(
  pg, paste(org, store, "trz2_ticket_discounts", sep = "_"), "ticket_id", "run_date_utc",
  with_ties = TRUE, columns = c("ticket_id", "run_date_utc", "product_id", "savings")
)
treez_products <- db_read_table_unique(
  pg, paste(org, store, "trz2_products", sep = "_"), "product_id", "last_updated_at",
  columns = c(
    "product_id", "last_updated_at", "name", "product_barcodes", "category_type",
    "classification", "brand", "amount", "uom"
  )
)
dbd(pg)

### READ FROM CONSOLIDATED
cats_map <- select(get_categories_mapping(org_uuid), -run_date_utc)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
customers <- dbGetQuery(
  pg, paste0("SELECT DISTINCT prot_customer_id, customer_id FROM ", org, "_customers")
)
proteus_order_lines <- dbReadTable(pg, "superiororganics_proteus_order_lines")
dbd(pg)

### BUILD

treez_order_lines <- build_trz2_order_lines(
  treez_tickets, treez_ticket_items, treez_ticket_discounts, treez_products, cats_map, class_map,
  org
) |>
  mutate(customer_id = paste0(store, "-", customer_id))

# Match proteus customer IDs to the new IDs.
proteus_order_lines <- rename(proteus_order_lines, prot_customer_id = customer_id) |>
  mutate(prot_customer_id = paste0("proteus_", prot_customer_id)) |>
  left_join(customers, by = "prot_customer_id") |>
  select(-prot_customer_id)

# Join ------------------------------------------------------------------------
order_lines <- bind_rows(treez_order_lines, proteus_order_lines)
order_lines$run_date_utc <- now("UTC")
### WRITE
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_order_lines")))
dbWriteTable(pg, paste0(org, "_order_lines"), order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
