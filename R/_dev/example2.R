library(rdleaflogix)

org <- "hello"
store <- "world"
auth  <- "MjhmMjJiYmU4OTk3NDM3OWEzMTI0YTU4YmY5YWViYTg="
ckey  <- "28f22bbe89974379a3124a58bf9aeba8"



cust_sum_indx <- get_ll_customers(org, store, auth, ckey, n = 10) |>
  ext_ll_customers(org, store) |>
  extract_index("customers")

stk_snap_indx <- get_ll_stocksnapshot(org, store, auth, ckey, n = 10) |>
  ext_ll_stocksnapshot(org, store) |>


transacts <- get_ll_transactions(org, store, auth, ckey, n = 10) |>
  ext_ll_transactions(org, store) |>
  bld_ll_transactions(org, store)

products_indx <- get_ll_products(org, store, auth, ckey, n = 10) |>
  ext_ll_products(org, store) |>






json <- get_ll_inventory(org, store, auth, ckey)
extracted <- ext_ll_inventory(json, org, store)


cs <- bld_ll_customer_summary(cust_sum_indx, org, store)
ss <- bld_ll_stock_snapshot(stk_snap_indx, org, store)
ps <- bld_ll_product_summary(products_indx, org, store)



