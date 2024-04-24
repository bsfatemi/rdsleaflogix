# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

org_uuid <- "447a88c2-ddfe-4a8f-b0f2-123db32299a6"

# Run ---------------------------------------------------------------------
# Connect and Read In Data
### READ FROM CABBAGE_PATCH
pg <- hcaconfig::dbc(cfg = "prod2", "cabbage_patch")
james_tickets <- pipelinetools::db_read_table_unique(
  pg, "jamestown_main_trz2_tickets", "order_number", "run_date_utc",
  columns = c("order_number", "run_date_utc", "customer_id", "ticket_id")
)
james_ticket_items <- pipelinetools::db_read_table_unique(
  pg, "jamestown_main_trz2_ticket_items", "ticket_id", "run_date_utc", with_ties = TRUE,
  columns = c("ticket_id", "product_id", "run_date_utc", "quantity", "price_sell", "price_total")
)
james_ticket_discounts <- pipelinetools::db_read_table_unique(
  pg, "jamestown_main_trz2_ticket_discounts", "ticket_id", "run_date_utc", with_ties = TRUE,
  columns = c("ticket_id", "run_date_utc", "product_id", "savings")
)
james_products <- pipelinetools::db_read_table_unique(
  pg, "jamestown_main_trz2_products", "product_id", "last_updated_at",
  columns = c(
    "product_id", "last_updated_at", "name", "product_barcodes", "category_type",
    "classification", "brand", "amount", "uom"
  )
)

best_tickets <- pipelinetools::db_read_table_unique(
  pg, "jamestown_mesa_trz2_tickets", "order_number", "run_date_utc",
  columns = c("order_number", "run_date_utc", "customer_id", "ticket_id")
)
best_ticket_items <- pipelinetools::db_read_table_unique(
  pg, "jamestown_mesa_trz2_ticket_items", "ticket_id", "run_date_utc", with_ties = TRUE,
  columns = c("ticket_id", "product_id", "run_date_utc", "quantity", "price_sell", "price_total")
)
best_ticket_discounts <- pipelinetools::db_read_table_unique(
  pg, "jamestown_mesa_trz2_ticket_discounts", "ticket_id", "run_date_utc", with_ties = TRUE,
  columns = c("ticket_id", "run_date_utc", "product_id", "savings")
)
best_products <- pipelinetools::db_read_table_unique(
  pg, "jamestown_mesa_trz2_products", "product_id", "last_updated_at",
  columns = c(
    "product_id", "last_updated_at", "name", "product_barcodes", "category_type",
    "classification", "brand", "amount", "uom"
  )
)
hcaconfig::dbd(pg)

### READ FROM CONSOLIDATED
cats_map <- pipelinetools::get_categories_mapping(org_uuid)
con <- hcaconfig::dbc(cfg = "prod2", "consolidated")
class_map <- DBI::dbReadTable(con, "product_classes")
hcaconfig::dbd(con)

## MAP CUSTOMER IDS
james_tickets$customer_id <- paste0("yuma-", james_tickets$customer_id)
best_tickets$customer_id <- paste0("mesa-", best_tickets$customer_id)

## BUILD
james <- hcatreez::build_trz2_order_lines(
  james_tickets, james_ticket_items, james_ticket_discounts, james_products, cats_map, class_map,
  "jamestown"
)

best <- hcatreez::build_trz2_order_lines(
  best_tickets, best_ticket_items, best_ticket_discounts, best_products, cats_map, class_map,
  "jamestown"
)

order_lines <- dplyr::bind_rows(james, best)
order_lines$run_date_utc <- lubridate::now(tzone = "UTC")
order_lines$org <- "jamestown"

# Write -------------------------------------------------------------------
con <- hcaconfig::dbc(cfg = "prod2", "consolidated")
DBI::dbBegin(con)
DBI::dbExecute(con, "TRUNCATE TABLE jamestown_order_lines")
DBI::dbWriteTable(con, "jamestown_order_lines", order_lines, append = TRUE)
DBI::dbCommit(con)
hcaconfig::dbd(con)
