# Consolidated Customers
# build_akerna_customers.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.
box::use(
  dplyr[
    coalesce, filter, group_by, if_else, left_join, mutate, rename, row_number, select, transmute
  ],
  lubridate[dyears, now, today, ymd, ymd_hms],
  pipelinetools[process_gender, process_phone, process_zipcode],
  stringr[str_detect, str_squish, str_to_lower, str_to_upper],
)
build_akerna_customers <- function(org, consumers, addresses) {
  consumers$customer_id <- as.character(consumers$id)

  addy <- addresses |>
    filter(primary == 1) |>
    group_by(customer_id = as.character(consumer_id)) |>
    filter(row_number(updated_at) == max(row_number(updated_at))) |>
    transmute(
      customer_id,
      city,
      state = province_code,
      zipcode = process_zipcode(postal_code),
      address_street1 = str_to_upper(street_address_1),
      address_street2 = str_to_upper(street_address_2),
      long_address = if_else(is.na(zipcode), NA_character_,
        str_squish(paste(
          coalesce(address_street1, ""),
          coalesce(address_street2, ""),
          coalesce(city, ""),
          coalesce(state, ""),
          zipcode
        ))
      )
    )

  # Clean and Create Columns
  df <- consumers |>
    mutate(
      source_run_date_utc = format(run_date_utc, "%Y-%m-%d %H:%M:%S"),
      first_name = str_to_upper(first_name),
      last_name = str_to_upper(last_name),
      full_name = paste(first_name, last_name),
      gender = process_gender(gender),
      birthday = anydate(birth_date),
      age = pmin(floor((today() - ymd(birthday)) / dyears(1)), 100, na.rm = TRUE),
      user_type = if_else(str_detect(type, "medical"), "MEDICAL", "RECREATIONAL"),
      created_at_utc = format(ymd_hms(created_at), "%Y-%m-%d %H:%M:%S"),
      last_updated_utc = format(ymd_hms(updated_at), "%Y-%m-%d %H:%M:%S"),
      phone = process_phone(
        phone_numbers_number
      ),
      email = str_to_upper(email_address),
      pos_is_subscribed = coalesce(preferred_contact == "text", FALSE),
      loyalty_pts = total_points,
      org = org,
      source_system = "akerna",
      geocode_type = "none"
    ) |>
    left_join(addy)

  return(df)
}
