# Seed Talent API PL
# read_jane.R
# (C) Happy Cababge Analytics, Inc. 2022

box::use(
  DBI[dbGetQuery],
  dplyr[left_join, mutate],
  glue[glue],
  pipelinetools[db_read_table_unique],
  purrr[map_df]
)

read_jane <- function(pg, org_short_name, stores) {
  jane <- map_df(names(stores), function(facility) {
    ihjane_names <- stores[[facility]]
    map_df(ihjane_names, function(store) {
      tbl1 <- paste(org_short_name, store, "iheartjane_reservation_products", sep = "_")
      prod <- dbGetQuery(pg, glue("
      SELECT
        cart_id as id,
        COUNT(DISTINCT product_id) items,
        SUM(CAST(checkout_price AS double precision) * count) item_lvl_list_price,
        SUM(CAST(discounted_checkout_price AS double precision) * count) item_lvl_subtotal,
        SUM(count) item_qty
      FROM {tbl1}
      GROUP BY cart_id
      "))
      tbl2 <- paste(org_short_name, store, "iheartjane_reservations", sep = "_")
      res <- db_read_table_unique(
        pg, tbl2,
        id = "id",
        sort_by = "run_date_utc",
        columns = c(
          "id",
          "amount",
          "created_at",
          "checked_out_time",
          "updated_at_time",
          "user_id",
          "pos_order_id",
          "customer_email_address",
          "customer_phone_number",
          "reservation_mode",
          "reservation_start_window",
          "status",
          "store_name"
        )
      ) |>
        mutate(user_id = as.character(user_id))
      res$jane_store <- store
      res$facility <- facility
      left_join(res, prod, by = "id")
    })
  })
  return(jane)
}
