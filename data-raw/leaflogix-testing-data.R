## Get full run results
# res <- run_get_extract_ll(24)

## construct data object
pl <- get_pipeline_args("leaflogix")
# f <- function(x) rbindlist(pl)[tab == x, .SD[sample(1:.N, 1)]][, .(org, store, auth, consumerkey)]

.pos_data <- list(
  treez = list(),
  webjoint = list(),
  blaze = list(),

  leaflogix = list(
    pos_api_keys  = setDT(read_leaflogix_creds())[1:25],
    pipeline_args = pl[1:25]
    # extract_index = res$data[sample(1:1000, 50, FALSE)],
    # extract_logs  = res$logs[51:100],
    # json = list(
    #   employees      = do.call(get_ll_employees,     f("employees")),
    #   customers      = do.call(get_ll_customers,     f("customers")),
    #   loyalty        = do.call(get_ll_loyalty,       f("loyalty")),
    #   transactions   = do.call(get_ll_transactions,  f("transactions")),
    #   products       = do.call(get_ll_products,      f("products")),
    #   brands         = do.call(get_ll_brands,        f("brands")),
    #   inventory      = do.call(get_ll_inventory,     f("inventory")),
    #   stocksnapshot  = do.call(get_ll_stocksnapshot, f("stocksnapshot")),
    #   categories     = do.call(get_ll_categories,    f("categories"))
    # )
  )
)



