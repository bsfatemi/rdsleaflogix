.pos_config <- rlang::new_environment(data = list(
  treez = list(),
  webjoint = list(),
  blaze = list(),
  leaflogix = list(
    endpoints = c(
      "employees",
      "loyalty",
      "products",
      "customers",
      "transactions",
      "inventory",
      "brands",
      "stocksnapshot",
      "categories"
    ),
    schema = ll_schema,
    db = list(
      consolidated = c(
        "customer_summary", "store_employees", "order_summary", "order_items", "customer_loyalty",
        "product_summary", "stock_lab_results", "stock_by_room", "stock_summary", "stock_snapshot",
        "brand_index", "category_index", "population"
      )
    )
  )
))
