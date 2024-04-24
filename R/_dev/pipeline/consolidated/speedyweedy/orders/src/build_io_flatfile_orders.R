# Consolidated Orders Data
# build_socalgreenbuddy_flatfile_orders.R
#
# (C) 2020 Happy Cabbage Analytics Inc.

#
## ORDER TOTALS -------------------------
#

build_io_flatfile_orders <- function(raw_orders, patients, org, store, store_name) {

  ### PARAMETERS
  source_system <- "indicaonline"

  # DeDup Orders
  orders_clean <- raw_orders %>%
    group_by(run_date_utc) %>%
    fill(type, order_number, order_source,
      patient, customer_type, created_at, status,
      fulfillment, metrc, metrc_status,
      .direction = "down"
    ) %>%
    fill(subtotal, sales_tax, shipping,
      total, total_discount_amount,
      .direction = "up"
    ) %>%
    mutate(
      line_key = if_else(is.na(qty), "Order Total", "Order Line")
    ) %>%
    ungroup()

  # DeDup Orders
  orders_dedup <- orders_clean %>%
    filter(line_key == "Order Total") %>%
    group_by(order_number) %>%
    filter(row_number(created_at) == max(row_number(created_at))) %>%
    ungroup()

  orders_dedup <- filter(
    orders_dedup, !status %in% c("Cancelled", "Declined", "Rejected", "Returned")
  )

  # Info Needed From Patients
  cust_ids <- patients %>%
    group_by(first_name, last_name) %>%
    filter(row_number(total_completed_orders) == max(row_number(total_completed_orders))) %>%
    ungroup() %>%
    transmute(
      patient = paste(first_name, last_name),
      customer_id = as.character(patient_id),
      delivery_order_address = if_else(
        is.na(zip),
        as.character(NA),
        str_squish(
          paste(
            coalesce(address, ""),
            coalesce(city, ""),
            coalesce(state, ""),
            coalesce(zip, "")
          )
        )
      )
    )

  # Clean Up Orders Info
  orders_info <- orders_dedup %>%
    mutate(
      org = !!org,
      source_system = !!source_system,
      source_run_date_utc = run_date_utc,
      order_id = order_number,
      order_type = if_else(str_detect(type, "Delivery"), "Delivery", "Retail"),
      sold_by = as.character(NA),
      sold_by_id = as.character(NA),
      order_time_local = if_else(
        !str_detect(created_at, "/"), created_at, format(mdy_hm(created_at), "%Y-%m-%d %H:%M:%S")
      ),
      order_time_utc = format(
        with_tz(ymd_hms(order_time_local, tz = "America/Los_Angeles"), "UTC"), "%Y-%m-%d %H:%M:%S"
      ),
      order_subtotal = as.numeric(total) - as.numeric(sales_tax),
      order_total = as.numeric(total),
      order_tax = as.numeric(sales_tax),
      order_discount = round(
        as.numeric(total) - (as.numeric(subtotal) + as.numeric(sales_tax)), 2
      )
    )

  # Delivery Specific Information
  delivery_info <- orders_info %>%
    filter(order_type == "Delivery") %>%
    transmute(
      order_id,
      delivery_order_received_local = order_time_local,
      delivery_order_started_local  = as.character(NA),
      delivery_order_complete_local = as.character(NA),
    )

  df <- orders_info %>%
    left_join(cust_ids, by = "patient") %>%
    left_join(delivery_info, by = "order_id") %>%
    filter(!is.na(customer_id)) %>%
    select(
      org,
      source_system,
      source_run_date_utc,
      order_id,
      customer_id,
      order_type,
      order_source,
      sold_by,
      sold_by_id,
      order_time_local,
      order_time_utc,
      delivery_order_address,
      delivery_order_received_local,
      delivery_order_started_local,
      delivery_order_complete_local,
      order_subtotal,
      order_tax,
      order_total,
      order_discount
    )

  df$run_date_utc <- lubridate::now("UTC")
  df$facility <- store
  df$order_facility <- store_name
  df$store_id <- hcaconfig::get_store_id(org = hcaconfig::lookupOrgGuid(org), store = store)
  return(df)
}
