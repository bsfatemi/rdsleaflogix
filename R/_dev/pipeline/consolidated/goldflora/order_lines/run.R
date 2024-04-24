# Consolidated order_lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[bind_rows, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd, lookupOrgGuid],
  hcatreez[build_trz2_order_lines],
  lubridate[now],
  pipelinetools[db_read_table_unique, get_categories_mapping]
)

# Vars ------------------------------------------------------------------------
org <- "goldflora"
org_uuid <- "aa32daf9-3f7d-45ee-b673-7c3879c6cb5b"
store <- "main"

# Run -------------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
customers <- dbGetQuery(
  pg, paste0("SELECT DISTINCT blaze_customer_id, customer_id FROM ", org, "_customers")
)
blaze_order_lines <- dbReadTable(pg, "goldflora_blaze_order_lines")
dbd(pg)

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

treez_order_lines <- build_trz2_order_lines(
  treez_tickets, treez_ticket_items, treez_ticket_discounts, treez_products, cats_map, class_map,
  org
) |>
  mutate(customer_id = paste0(store, "-", customer_id))

# Match Blaze customer IDs to the new IDs.
blaze_order_lines <- rename(blaze_order_lines, blaze_customer_id = customer_id) |>
  mutate(blaze_customer_id = paste0("blaze_", blaze_customer_id)) |>
  left_join(customers, by = "blaze_customer_id") |>
  select(-blaze_customer_id)

# Join ------------------------------------------------------------------------
order_lines <- bind_rows(treez_order_lines, blaze_order_lines)
order_lines$run_date_utc <- now("UTC")

# Write -----------------------------------------------------------------------
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste0(org, "_order_lines")))
dbWriteTable(pg, paste0(org, "_order_lines"), order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
