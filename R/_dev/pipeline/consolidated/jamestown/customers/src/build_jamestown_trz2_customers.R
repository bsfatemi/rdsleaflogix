# Consolidated Customers
# build_jamestown_trz2_customers.R
#
# (C) 2020 Happy Cabbage Analytics, Inc.

build_jamestown_trz2_customers <- function(trz_customers) {
  # PARAMS
  org <- "jamestown"
  source_system <- "treez2.0"
  geocode_type <- "none"
  # Expand Nested Data
  raw <- trz_customers %>%
    mutate(
      addresses = map(addresses, ~ fromJSON(coalesce(.x, "[]"), simplifyDataFrame = FALSE))
    ) %>%
    unnest_longer(addresses) %>%
    hoist(addresses,
      address_type    = "type",
      address_street1 = "street1",
      address_street2 = "street2",
      address_city    = "city",
      address_state   = "state",
      address_primary = "primary",
      address_zipcode = "zipcode"
    ) %>%
    group_by(customer_id) %>%
    arrange(address_primary) %>%
    filter(row_number(run_date_utc) == max(row_number(run_date_utc))) %>%
    ungroup()

  # Clean and Create Columns
  df <- raw %>%
    mutate_at(
      vars(address_street1, address_street2, address_city, address_state, address_zipcode),
      ~ if_else(.x == "", as.character(NA), .x)
    ) %>%
    mutate(
      zipcode = pipelinetools::process_zipcode(address_zipcode),
      long_address = if_else(
        is.na(zipcode),
        as.character(NA),
        str_squish(paste(
          address_street1, coalesce(address_street2, ""), address_city, address_state, zipcode
        ))
      ),
      first_name = str_to_upper(first_name),
      last_name = str_to_upper(last_name),
      full_name = paste(first_name, last_name),
      age = pmin(floor((today() - ymd(birthday)) / dyears(1)), 100, na.rm = TRUE),
      org = !!org,
      source_system = !!source_system,
      user_type = if_else(str_detect(patient_type, "MEDICAL"), "MEDICAL", "RECREATIONAL"),
      created_at_utc = format(ymd_hms(signup_date), "%Y-%m-%d %H:%M:%S"),
      last_updated_utc = format(ymd_hms(last_update), "%Y-%m-%d %H:%M:%S"),
      phone = pipelinetools::process_phone(phone),
      source_run_date_utc = format(run_date_utc, "%Y-%m-%d %H:%M:%S"),
      email = str_to_upper(email),
      gender = pipelinetools::process_gender(gender),
      pos_is_subscribed = coalesce(!as.logical(opt_out), FALSE)
    )

  # Select the Columns We Want To Keep.
  customers <- df %>%
    filter(!str_detect(first_name, "TEST")) %>%
    select(
      org,
      source_system,
      customer_id,
      created_at_utc,
      last_updated_utc,
      source_run_date_utc,
      phone,
      first_name,
      last_name,
      full_name,
      gender,
      birthday,
      age,
      email,
      long_address,
      address_street1,
      address_street2,
      city = address_city,
      state = address_state,
      zipcode,
      user_type,
      pos_is_subscribed
    ) %>%
    distinct()

  # Last things
  output <- customers %>%
    mutate(geocode_type = geocode_type) %>%
    mutate_if(is.character, ~ (if_else(. == "", as.character(NA), .)))


  return(output)
}
