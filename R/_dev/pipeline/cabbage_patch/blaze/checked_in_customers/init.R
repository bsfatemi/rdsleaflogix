# Libraries.
box::use(
  dplyr[
    add_count, arrange, as_tibble, bind_rows, filter, mutate, select, tibble, transmute_at, vars
  ],
  fs[path_ext, path_package],
  hcablaze[g, get_api_keys],
  hcaconfig[orgTimeZone],
  httr[content],
  jsonlite[fromJSON],
  lubridate[as_datetime],
  pipelinetools[process_phone],
  purrr[map_dfr],
  stringr[str_remove],
  yaml[read_yaml]
)

# Format Blaze credentials to match with yaml credentials below
get_ez_credentials <- function() {
  get_api_keys() |>
    add_count(org_uuid, name = "multi_store") |>
    mutate(
      multi_store = multi_store > 1,
      org_tz = sapply(org_uuid, orgTimeZone)
    ) |>
    select(
      value = apikey,
      org_uuid,
      facility = store_short_name,
      org_tz,
      multi_store
    )
}

# Format Blaze credentials to match with ez credentials above
get_blaze_credentials <- function() {
  credentials_file <- "src-blaze.yml"
  credentials <- path_package(
    "hcablaze", "extdata", path_ext(credentials_file), credentials_file
  ) |>
    read_yaml()
  credentials <- credentials[[1]]$creds$clients
  map_dfr(credentials, function(org_credentials) {
    org_uuid <- names(org_credentials)
    org_tz <- orgTimeZone(org_uuid)
    org_credentials <- org_credentials[[1]]
    # Unnamed stores case.
    if (is.null(names(org_credentials))) {
      org_credentials <- list(main = org_credentials)
    }
    map_dfr(names(org_credentials), function(facility) {
      as_tibble(org_credentials[[facility]]) |>
        mutate(org_uuid = !!org_uuid, facility = !!facility, org_tz = !!org_tz)
    })
  }) |>
    add_count(org_uuid, name = "multi_store") |>
    mutate(multi_store = multi_store > 1) |>
    bind_rows(get_ez_credentials())
}



# Get checked-in customers.
get_checked_in <- function(start_time, apikey) {
  # Format the time to string.
  st_str <- format(as.numeric(start_time) * 1000, scientific = FALSE)
  # Set starting values.
  qll <- list(limit = 500, skip = 0, startDate = st_str)
  checked_in <- tibble()
  parsed <- 420 # Any value, to enter the `while`.
  pull_res <- try(
    {
      # Perform the API query.
      while (length(parsed) > 0) {
        resp <- g("/members/days", apikey$value, query = qll)
        parsed <- fromJSON(content(resp, encoding = "UTF-8", as = "text"))$values
        if (is.data.frame(parsed)) {
          checked_in <- bind_rows(checked_in, parsed)
        }
        qll$skip <- qll$skip + qll$limit
      }
    },
    silent = TRUE
  )
  if (
    inherits(pull_res, "try-error") || nrow(checked_in) == 0 ||
      !"lastVisitDate" %in% colnames(checked_in)
  ) {
    print(paste0("No checked-in customers for ", apikey$facility))
    return(tibble())
  }
  checked_in <- mutate(
    checked_in,
    org_uuid = apikey$org_uuid,
    facility = apikey$facility,
    customer_id = as.character(id),
    last_visit_date = as_datetime(as.numeric(lastVisitDate) / 1000, tz = apikey$org_tz),
    full_name = paste(trimws(firstName), trimws(lastName)),
    phone = process_phone(primaryPhone)
  ) |>
    filter(last_visit_date >= start_time) |>
    arrange(full_name)
  transmute_at(
    checked_in,
    vars(org_uuid, facility, customer_id, full_name, email, phone),
    as.character
  )
}
