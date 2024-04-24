# Consolidated order_lines
# run.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

box::use(
  DBI[dbBegin, dbCommit, dbExecute, dbGetQuery, dbReadTable, dbWriteTable],
  dplyr[filter, left_join, mutate, one_of, select],
  glue[glue],
  hcablaze[build_blaze_order_lines],
  hcaconfig[dbc, dbd],
  pipelinetools[db_read_table_unique, get_categories_mapping],
  purrr[map_dfr],
  stringr[str_detect]
)

(db_env <- Sys.getenv("HCA_ENV", "prod2"))

# Vars --------------------------------------------------------------------
org <- "demoorg"
org_uuid <- "fcfaa275-9529-490d-a156-858892aaf365"
stores <- c("hollywood", "longbeach", "marinadelrey", "noho")

# Run ---------------------------------------------------------------------
cats_map <- get_categories_mapping(org_uuid)
pg <- dbc(db_env, "consolidated")
class_map <- dbReadTable(pg, "product_classes")
dbd(pg)

pg <- dbc(db_env, "cabbage_patch")
order_lines <- map_dfr(seq_along(stores), function(i) {
  store <- stores[[i]]
  cart_items <- db_read_table_unique(
    pg, paste(org, store, "blaze_transactions_cart_items", sep = "_"), c("transaction_id", "id"),
    "run_date_utc", columns = c(
      "id", "transaction_id", "run_date_utc", "quantity", "calc_discount", "final_price",
      "calc_tax", "cost", "product_id"
    )
  )
  transactions <- db_read_table_unique(
    pg, paste(org, store, "blaze_transactions", sep = "_"), "id", "run_date_utc", columns = c(
      "id", "member_id", "status", "cart_total_discount", "cart_total_calc_tax", "cart_total_fees",
      "run_date_utc"
    )
  )
  # Replace the product and brand names for MMD ~> House Brand.
  products <- db_read_table_unique(
    pg, paste(org, store, "blaze_products", sep = "_"), "id", "run_date_utc", columns = c(
      "id", "run_date_utc", "brand_id", "name", "weight_per_unit", "unit_value", "category_name",
      "flower_type", "sku"
    )
  ) |>
    mutate(name = gsub("mmd", "House Brand", name, ignore.case = TRUE))
  brands <- db_read_table_unique(
    pg, paste(org, store, "blaze_brands", sep = "_"), "id", "run_date_utc",
    columns = c("id", "run_date_utc", "name")
  ) |>
    mutate(name = gsub("mmd", "House Brand", name, ignore.case = TRUE))
  build_blaze_order_lines(
    cart_items, transactions, products, brands, cats_map, class_map, org, store, org_uuid
  )
})
# Read already anonymized data.
anonymized_customers <- dbGetQuery(pg, glue(
  "SELECT DISTINCT real_customer_id, customer_id FROM {org}_anonymized_customers"
))
dbd(pg)

# Anonymize ---------------------------------------------------------------
# Replace real data with the anonymized data.
order_lines <- mutate(order_lines, real_customer_id = as.character(customer_id)) |>
  select(-one_of(setdiff(colnames(anonymized_customers), "real_customer_id"))) |>
  left_join(anonymized_customers, by = "real_customer_id") |>
  select(-real_customer_id)

# Last tweaks.
no_samples <- "DISPLAY|PROMO|SAMPLE"
order_lines <- order_lines |>
  filter(!str_detect(brand_name, no_samples), !str_detect(product_name, no_samples)) |>
  mutate(customer_id = as.character(customer_id))

# Write -------------------------------------------------------------------
pg <- dbc(db_env, "consolidated")
dbBegin(pg)
dbExecute(pg, paste("TRUNCATE TABLE", paste(org, "order_lines", sep = "_")))
dbWriteTable(pg, paste(org, "order_lines", sep = "_"), order_lines, append = TRUE)
dbCommit(pg)
dbd(pg)
