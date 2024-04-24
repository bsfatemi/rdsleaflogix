# Libraries.
box::use(
  dplyr[add_count, arrange, as_tibble, if_else, mutate, tibble, transmute_at, vars, transmute],
  hcaconfig[orgTimeZone],
  hcaleaflogix[get_api_keys],
  httr[add_headers, content, GET],
  jsonlite[fromJSON],
  pipelinetools[process_phone],
  purrr[map, map_dfr],
  stringr[str_remove]
)

# Format nicely all stores' credentials.
get_leaflogix_credentials <- function() {
  get_api_keys() |>
    transmute(
      org_uuid,
      consumerkey,
      auth = paste("Basic", auth),
      org_tz = map(org_uuid, orgTimeZone),
      facility = store_short_name
    )
}

# Get checked-in customers.
get_checked_in <- function(apikey) {
  # Perform the API query.
  pull_res <- try(
    {
      # Get the checked-in customers.
      checked_in <- fromJSON(content(GET(
        "https://publicapi.leaflogix.net/guestlist",
        add_headers(ConsumerKey = apikey$consumerkey, Authorization = apikey$auth)
      ), encoding = "UTF-8", as = "text"))
      # Get the checked-in customers personal information.
      checked_in <- map_dfr(unique(checked_in$customerId), function(customer_id) {
        fromJSON(content(GET(
          "https://publicapi.leaflogix.net/customer/customers",
          query = list(customerID = customer_id),
          add_headers(ConsumerKey = apikey$consumerkey, Authorization = apikey$auth)
        ), encoding = "UTF-8", as = "text"))
      })
    },
    silent = TRUE
  )
  # Check for errors.
  if (inherits(pull_res, "try-error") || nrow(checked_in) == 0) {
    print(paste0("No checked-in customers for ", apikey$facility))
    return(tibble())
  }
  checked_in <- mutate(
    checked_in,
    org_uuid = apikey$org_uuid,
    facility = apikey$facility,
    customer_id = as.character(customerId),
    middleName = trimws(middleName),
    full_name = paste0(
      trimws(firstName), " ",
      if_else(is.na(middleName) | nchar(middleName) == 0, "", paste0(middleName, " ")),
      trimws(lastName)
    ),
    phone = process_phone(phone),
    email = emailAddress
  ) |>
    arrange(full_name)
  transmute_at(
    checked_in,
    vars(org_uuid, facility, customer_id, full_name, email, phone),
    as.character
  )
}
