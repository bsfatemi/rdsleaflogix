# Libraries.
box::use(
  dplyr[
    add_count, arrange, as_tibble, bind_rows, filter, if_else, mutate, select, tibble, transmute_at,
    vars
  ],
  fs[path_ext, path_package],
  hcaconfig[orgTimeZone],
  hcatreez[extractTreez, get_api_keys, getPageTreez],
  lubridate[as_datetime],
  pipelinetools[process_phone],
  purrr[map_dfr],
  stringr[str_glue, str_remove],
  utils[URLencode],
  yaml[read_yaml]
)

# Format EZ credentials to match with yaml credentials below
get_ez_credentials <- function() {
  get_api_keys() |>
    add_count(org_uuid, name = "multi_store") |>
    mutate(
      multi_store = multi_store > 1,
      org_tz = sapply(org_uuid, orgTimeZone)
    ) |>
    select(
      apikey,
      disp_name = dispensary_name,
      org_uuid,
      facility = store_short_name,
      org_tz,
      multi_store
    )
}


# Format nicely all stores' credentials.
get_treez_credentials <- function() {
  credentials_file <- "src-treez.yml"
  credentials <- path_package(
    "hcatreez", "extdata", path_ext(credentials_file), credentials_file
  ) |>
    read_yaml()

  credentials <- credentials[[1]]$creds$clients
  map_dfr(credentials, function(org_credentials) {
    org_uuid <- names(org_credentials)
    org_tz <- orgTimeZone(org_uuid)
    org_credentials <- org_credentials[[1]]
    # Unnamed stores case.
    if (length(setdiff(c("apikey", "disp_name"), names(org_credentials))) == 0) {
      org_credentials <- list(main = org_credentials)
    }
    map_dfr(names(org_credentials), function(facility) {
      as_tibble(org_credentials[[facility]]) |>
        mutate(org_uuid = !!org_uuid, facility = str_remove(facility, ".*_"), org_tz = !!org_tz)
    })
  }) |>
    add_count(org_uuid, name = "multi_store") |>
    mutate(multi_store = multi_store > 1) |>
    bind_rows(get_ez_credentials())
}

# Get checked-in customers.
get_checked_in <- function(start_time, apikey, client_id) {
  # Format the time to string.
  eob_str <- URLencode(paste0(
    format(start_time, "%Y-%m-%dT%H:%M:%S.000"),
    substr(format(start_time, "%z"), 1, 3), ":", substr(format(start_time, "%z"), 4, 5)
  ), reserved = TRUE)
  # Perform the API query.
  checked_in <- try(extractTreez(getPageTreez(
    apikey$apikey, client_id, str_glue("/customer/lastUpdated/after/{eob_str}"),
    apikey$disp_name
  ), "customers"), silent = TRUE)
  # Check for errors.
  if (
    inherits(checked_in, "try-error") || nrow(checked_in) == 0 ||
      !"last_visit_date" %in% colnames(checked_in)
  ) {
    print(paste0("No checked-in customers for ", apikey$disp_name))
    return(tibble())
  }
  checked_in <- mutate(
    checked_in,
    org_uuid = apikey$org_uuid,
    facility = apikey$facility,
    last_visit_date = as_datetime(last_visit_date, tz = apikey$org_tz),
    middle_name = trimws(middle_name),
    full_name = paste0(
      trimws(first_name), " ",
      if_else(is.na(middle_name) | nchar(middle_name) == 0, "", paste0(middle_name, " ")),
      trimws(last_name)
    ),
    phone = process_phone(phone)
  ) |>
    filter(last_visit_date >= start_time) |>
    arrange(full_name)
  # If it has more than one store, then the `customer_id` will be preceded by "FACILITY-".
  if (apikey$multi_store) {
    checked_in$customer_id <- paste0(checked_in$facility, "-", checked_in$customer_id)
  }
  transmute_at(
    checked_in,
    vars(org_uuid, facility, customer_id, full_name, email, phone),
    as.character
  )
}
