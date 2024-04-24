# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.
box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbExistsTable, dbReadTable, dbWriteTable],
  dplyr[distinct, filter, mutate],
  hcaconfig[dbc, dbd],
  hcatreez[build_trz2_order_lines],
  lubridate[days, today],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr]
)

# Vars ------------------------------------------------------------------
org <- "fstreet"
org_uuid <- hcaconfig::lookupOrgGuid(org)
stores <- hcaconfig::get_org_stores(org_uuid)$facility
update_day <- format(today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1)))
out_table <- "fstreet_trz2_order_lines"
# Run ---------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
order_lines <- map_dfr(stores, function(store) {
  products <- db_read_table_unique(
    pg, paste(org, store, "trz2_products", sep = "_"), "product_id", "last_updated_at",
    columns = c(
      "product_id", "last_updated_at", "name", "product_barcodes", "category_type",
      "classification", "brand", "amount", "uom", "run_date_utc"
    )
  )

  # Checks to see if any new products were added.
  new_products <- filter(products, run_date_utc >= update_day) |>
    distinct(product_id)
  # If any new product, create a temp table to be used for selecting tickets to update product info.
  if (nrow(new_products) > 0) {
    dbWriteTable(
      pg, paste(org, store, "trz2_products_temp", sep = "_"), new_products,
      temporary = TRUE
    )
    ticket_items_filters <- paste0(
      'ticket_id IN (SELECT DISTINCT ticket_id from "',
      paste(org, store, "trz2_ticket_items", sep = "_"),
      '" WHERE (run_date_utc >= \'', update_day, '\' OR product_id IN (SELECT product_id FROM "',
      paste(org, store, "trz2_products_temp", sep = "_"), '")))'
    )
  } else {
    # If no new products will only filter on new tickets.
    ticket_items_filters <- paste0("run_date_utc >= '", update_day, "'")
  }

  ticket_items <- db_read_table_unique(
    pg, paste(org, store, "trz2_ticket_items", sep = "_"), "ticket_id", "run_date_utc",
    with_ties = TRUE,
    columns = c("ticket_id", "product_id", "run_date_utc", "quantity", "price_sell", "price_total"),
    extra_filters = ticket_items_filters
  )
  # If no new cabbage patch changes return NULL so wont error on build.
  if (nrow(ticket_items) == 0) {
    return(NULL)
  }

  # Write temporary table to query updated `ticket_ids`.
  dbWriteTable(
    pg, paste(org, store, "trz2_tickets_temp", sep = "_"), distinct(ticket_items, ticket_id),
    temporary = TRUE
  )
  ticket_filters <- paste0(
    'ticket_id IN (SELECT ticket_id FROM "',
    paste(org, store, "trz2_tickets_temp", sep = "_"), '")'
  )

  tickets <- db_read_table_unique(
    pg, paste(org, store, "trz2_tickets", sep = "_"), "order_number", "run_date_utc",
    columns = c("order_number", "run_date_utc", "customer_id", "ticket_id"),
    extra_filters = ticket_filters
  )

  ticket_discounts <- db_read_table_unique(
    pg, paste(org, store, "trz2_ticket_discounts", sep = "_"), "ticket_id", "run_date_utc",
    with_ties = TRUE, columns = c("ticket_id", "run_date_utc", "product_id", "savings"),
    extra_filters = ticket_filters
  )
  build_trz2_order_lines(
    tickets, ticket_items, ticket_discounts, products, cats_map, class_map, org
  ) |>
    mutate(customer_id = paste0(store, "-", customer_id))
})
dbd(pg)

# WRITE -------------------------------------------------------------------
if (nrow(order_lines) > 0) {
  pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
  dbWriteTable(
    pg, paste(org, "order_lines_temp", sep = "_"), distinct(order_lines, order_id),
    temporary = TRUE
  )
  dbBegin(pg)
  if (dbExistsTable(pg, out_table)) {
    dbExecute(pg, paste0(
      'DELETE FROM "', out_table, '" WHERE order_id IN (SELECT order_id FROM "',
      paste(org, "order_lines_temp", sep = "_"), '")'
    ))
  }
  dbWriteTable(pg, out_table, order_lines, append = TRUE)
  dbCommit(pg)
  dbd(pg)
}
