# Integrated Customers
# build_customers.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  dplyr[any_of, arrange, coalesce, filter, if_else, left_join, mutate, mutate_if, select],
  hcapipelines[plAppDataIndex],
  pipelinetools[process_phone]
)

build_customers <- function(customers, opt_outs) {
  pos_optouts <- filter(plAppDataIndex(), !pos_optins)$short_name

  # Create Data
  customers <- customers |>
    mutate(pos_is_subscribed = if_else(org %in% pos_optouts, FALSE, pos_is_subscribed)) |>
    left_join(opt_outs, by = c("phone", "org")) |>
    mutate(
      is_subscribed = coalesce(is_subscribed, pos_is_subscribed, TRUE),
      tot_sms_received = coalesce(tot_sms_received, 0L),
      phone = process_phone(phone)
    ) |>
    mutate_if(is.character, ~ (if_else(. == "", as.character(NA), .))) |>
    arrange(phone)

  # GLOBAL ZIPCODE CLEANUP
  city_zip <- select(zipcodeR::zip_code_db, zipcode, zipcity = major_city)
  left_join(customers, city_zip, by = "zipcode") |>
    mutate(city = toupper(coalesce(zipcity, city))) |>
    select(-zipcity)
}
