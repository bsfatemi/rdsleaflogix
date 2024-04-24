# Consolidated Order Lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExistsTable, dbExecute, dbReadTable, dbWriteTable],
  dplyr[bind_rows, distinct, filter, group_by, mutate, rename, select],
  hcaconfig[dbc, dbd],
  hcaleaflogix[build_ll_order_lines, build_ll_products],
  logger[log_info],
  lubridate[days, now, today],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr],
  stringr[str_glue]
)

# Vars ------------------------------------------------------------------------
org <- args$org_short_name
org_uuid <- args$org_uuid
stores <- args$stores_short_names[[1]]
out_table <- args$order_lines
update_day <- today("UTC") - days(Sys.getenv("PREVIOUS_DAYS", 1))
extra_filter <- paste0("run_date_utc >= '", update_day, "'")

# Run -------------------------------------------------------------------------
log_info("Starting consolidated order_lines build for org: ", org)

cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
class_map <- dbReadTable(pg, "product_classes")
dbd(pg)

pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
products <- map_dfr(stores, function(store) {
  log_info("Preparing cabbage_patch products table for store: ", store)
  products <- db_read_table_unique(
    pg, paste(org, store, "ll_products", sep = "_"), "productId", "run_date_utc",
    columns = c(
      '"productId"', "sku", '"productName"', '"brandId"', '"brandName"', "size",
      '"strainType"', "category", "run_date_utc", '"lastModifiedDateUTC"'
    )
  )
  build_ll_products(products, cats_map, class_map)
}) |>
  group_by(product_id) |>
  filter(lastModifiedDateUTC == max(lastModifiedDateUTC)) |>
  distinct(product_id, .keep_all = TRUE) |>
  select(-lastModifiedDateUTC)

order_lines <- map_dfr(stores, function(store) {
  log_info("Reading in cabbage_patch line_items table for store: ", store)
  line_items <- db_read_table_unique(
    pg, paste(org, store, "ll_line_items", sep = "_"), "transactionItemId", "run_date_utc",
    columns = c(
      '"transactionId"', '"transactionItemId"', '"returnedByTransactionId"',
      '"unitWeightUnit"', '"quantity"', '"unitPrice"', '"totalDiscount"',
      '"productId"', '"totalPrice"', "run_date_utc"
    ),
    extra_filters = extra_filter
  )
  if (nrow(line_items) == 0) {
    return(NULL)
  }
  log_info("Reading in cabbage_patch transactions table for store: ", store)
  transactions <- db_read_table_unique(
    pg, paste(org, store, "ll_transactions", sep = "_"), "transactionId", "run_date_utc",
    columns = c(
      '"transactionId"', '"customerId"', '"isVoid"', '"transactionType"', "total", '"isReturn"',
      '"revenueFeesAndDonations"', '"totalBeforeTax"', '"tax"', '"totalDiscount"',
      "run_date_utc"
    ),
    extra_filters = extra_filter
  )
  log_info("Reading in cabbage_patch taxes table for store: ", store)
  taxes <- db_read_table_unique(
    pg, paste(org, store, "ll_line_items_taxes", sep = "_"), c("transactionItemId", "rateName"),
    "run_date_utc",
    columns = c('"transactionItemId"', "amount", '"rateName"', "run_date_utc"),
    extra_filters = extra_filter
  )
  log_info("Building consolidated order_lines using prepared tables above for store: ", store)
  build_ll_order_lines(
    line_items, transactions, taxes, org
  )
})
dbd(pg)

# WRITE -----------------------------------------------------------------------
if (nrow(order_lines) > 0) {
  log_info("Checking for updates to old orders.")
  products_out <- paste(org, "products_temp", sep = "_")
  order_lines$run_date_utc <- now(tzone = "UTC")
  pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "consolidated")
  dbWriteTable(
    pg, paste(org, "order_lines_temp", sep = "_"), distinct(order_lines, order_id),
    temporary = TRUE
  )
  # temp table to do product updates
  dbWriteTable(
    pg, products_out, products,
    temporary = TRUE
  )
  dbBegin(pg)
  log_info("Deleting order_lines rows that have had an update since last run. (refunds, etc.)")
  if (dbExistsTable(pg, out_table)) {
    dbExecute(pg, paste0(
      'DELETE FROM "', out_table, '" WHERE order_id IN (SELECT order_id FROM "',
      paste(org, "order_lines_temp", sep = "_"), '")'
    ))
  } else {
    # If DB doesn't exist this must be the first time creating the table so will need to add the
    # product columns so the update script wont error.
    order_lines <- order_lines |> mutate(
      raw_product_class = NA_character_,
      product_class = NA_character_,
      product_sku = NA_character_,
      product_name = NA_character_,
      brand_id = NA_character_,
      brand_name = NA_character_,
      product_unit_count = as.numeric(NA),
      product_category_name = NA_character_,
      raw_category_name = NA_character_,
      raw_brand_name = NA_character_,
      raw_product_name = NA_character_,
    )
  }
  log_info("Appending new order_lines to database table.")
  dbWriteTable(pg, out_table, order_lines, append = TRUE)
  # Compares new product data to existing product data in order lines.
  # If any product data we use are different then those rows will update.
  log_info("Updating product information in order_lines table if there are updates.")
  dbExecute(pg, str_glue("UPDATE {out_table}
SET raw_product_class  = p.raw_product_class,
product_class  = p.product_class,
product_sku    = p.product_sku,
product_name = p.product_name,
brand_id = p.brand_id,
brand_name = p.brand_name,
product_unit_count = p.product_unit_count,
product_category_name = p.product_category_name,
raw_category_name = p.raw_category_name,
raw_brand_name = p.raw_brand_name,
raw_product_name = p.raw_product_name
  FROM {products_out} p
WHERE {out_table}.product_id = p.product_id AND EXISTS (
  SELECT {out_table}.raw_product_class, {out_table}.product_class, {out_table}.product_sku,
  {out_table}.product_name, {out_table}.brand_id, {out_table}.brand_name,
  {out_table}.product_unit_count, {out_table}.product_category_name, {out_table}.raw_category_name,
  {out_table}.raw_brand_name, {out_table}.raw_product_name
  EXCEPT
  SELECT p.raw_product_class, p.product_class, p.product_sku, p.product_name, p.brand_id,
  p.brand_name, p.product_unit_count, p.product_category_name, p.raw_category_name,
  p.raw_brand_name, p.raw_product_name
)"))
  dbCommit(pg)
  dbd(pg)
}
log_info("Consolidated order_lines script finished for org: ", org)
