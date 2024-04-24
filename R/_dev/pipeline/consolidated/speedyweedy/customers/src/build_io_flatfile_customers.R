build_io_flatfile_customers <- function(raw_file, org) {

  # Set Parameters
  geocode_type <- "none"

  # Clean data, and take the most recent record by customer
  raw_file <- raw_file |>
    group_by(patient_id) |>
    filter(row_number(run_date_utc) == max(row_number(run_date_utc))) |>
    ungroup()

  df <- raw_file |>
    mutate(
      customer_id = as.character(patient_id),
      full_name = paste(coalesce(first_name, ""), coalesce(last_name, "")),
      phone = pipelinetools::process_phone(cell_phone),
      full_name = str_squish(paste(first_name, last_name)),
      birthday = anydate(dob),
      age = pmin(floor((today() - ymd(birthday)) / dyears(1)), 100, na.rm = TRUE),
      gender = pipelinetools::process_gender(gender),
      user_type = if_else(patient_type == "Adult use 21+", "RECREATIONAL", "MEDICAL"),
      created_at_utc = format(ymd(date_joined_collective) + seconds(1), "%Y-%m-%d %H:%M:%S"),
      last_updated_utc = format(
        coalesce(ymd_hms(last_order_date), run_date_utc), "%Y-%m-%d %H:%M:%S"
      ),
      source_run_date_utc = format(run_date_utc, "%Y-%m-%d %H:%M:%S"),
      email = str_to_upper(email),
      pos_is_subscribed = if_else(phone_consent_given == "Y", TRUE, FALSE, missing = FALSE),
      address_street1 = str_to_upper(address),
      address_street2 = as.character(NA),
      long_address = if_else(
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
  # Fix remaining birthdays that couldn't be parsed before.
  df$birthday[is.na(df$birthday)] <- as.character(
    as_date(raw_file$dob[is.na(df$birthday)], format = "%m/%d/%Y")
  )
  df$created_at_utc[is.na(df$created_at_utc)] <- format(
    as_date(raw_file$date_joined_collective[is.na(df$created_at_utc)], format = "%m/%d/%Y") +
      seconds(1),
    "%Y-%m-%d %H:%M:%S"
  )

  # Select the Columns We Want To Keep.
  customers <- df |>
    select(
      customer_id,
      last_updated_utc,
      created_at_utc,
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
      city,
      state,
      zipcode = zip,
      user_type,
      pos_is_subscribed
    ) |>
    distinct()

  # Last things
  output <- customers |>
    mutate(geocode_type = geocode_type,
           org = org) |>
    mutate_if(is.character, ~ (if_else(. == "", as.character(NA), .)))

  return(output)
}
