# Sirius App Data
# build_sirius_appdata.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  data.table[`:=`, fcoalesce, setDT],
  dplyr[`%>%`, coalesce, filter, group_by, left_join, mutate, select, ungroup],
  lubridate[as_datetime, date],
  stringr[str_to_upper]
)

build_sirius_appdata <- function(population) {
  setDT(population)

  # clean and process
  population[, "order_time_local" := as_datetime(order_time_local)]
  population[, "delivery_order_received_local" := as_datetime(delivery_order_received_local)]
  population[, "delivery_order_started_local" := as_datetime(delivery_order_started_local)]
  population[, "delivery_order_complete_local" := as_datetime(delivery_order_complete_local)]

  # order summary
  population[, `:=`(
    "full_name" = paste(first_name, last_name),
    "gender" = fcoalesce(gender, "U"),
    "city" = customer_city,
    "total" = order_total,
    "subtotal" = order_subtotal,
    "addr" = delivery_order_address,
    "order_time" = order_time_local,
    "received" = delivery_order_received_local,
    "deliver_city" = delivery_order_city,
    "delivery_zip" = delivery_order_zipcode,
    "customer_zip" = customer_zipcode,
    "dur_process" = as.numeric(difftime(
      delivery_order_started_local, delivery_order_received_local,
      units = "mins"
    )),
    "dur_total" = as.numeric(difftime(
      delivery_order_complete_local, delivery_order_received_local,
      units = "mins"
    ))
  )]

  keep_cols <- c(
    "customer_id", "phone", "email", "full_name", "gender", "age", "birthday",
    "city", "order_id", "sold_by_id", "sold_by", "order_facility", "order_type",
    "total", "subtotal", "addr", "order_time", "received", "deliver_city", "delivery_zip",
    "customer_zip", "dur_process", "dur_total", "org", "order_line_id", "product_id", "brand_id",
    "category3", "product_name", "product_class", "product_qty",
    "product_unit_count", "brand_name", "order_line_total", "order_line_subtotal",
    "order_line_list_price", "order_line_tax", "order_source", "order_line_discount"
  )

  pop <- population[, keep_cols, with = FALSE]

  pop <- group_by(pop, customer_id) %>%
    mutate(is_first_order = date(order_time) == min(date(order_time))) %>%
    ungroup()
  city_zip <- select(zipcodeR::zip_code_db, delivery_zip = zipcode, zipcity = major_city)
  left_join(pop, city_zip, by = "delivery_zip") %>%
    mutate(city = str_to_upper(coalesce(city, zipcity, deliver_city))) %>%
    select(-zipcity) %>%
    filter(!is.na(order_facility) & !is.na(order_time))
}
