box::use(
  DBI[dbReadTable],
  dplyr[bind_rows, filter, left_join, mutate, rename, select],
  hcaconfig[dbc, dbd]
)

# Load DemoOrg's anonymized data from our DBs, and replace with real data.
#
anonymize_demoorg <- function(org_short, store_credentials, checked_in) {
  # Load anonymized data for customers.
  pg <- dbc(Sys.getenv("HCA_ENV", "prod2"), "cabbage_patch")
  anonymized_customers <- dbReadTable(pg, paste0(org_short, "_anonymized_customers")) |>
    select(real_customer_id, customer_id, full_name, email, phone)
  dbd(pg)
  # By default, we have four checked-in customers per facility, one per category.
  data.frame(
    org_uuid = store_credentials$org_uuid,
    facility = c("hollywood", "noho", "longbeach", "marinadelrey"),
    customer_id = c(
      # Most orders.
      "5f8cb80c74be1b08c680c587", "5ec86b8883447d08c76bb45a", "5e665c8ae0faca07f56e6c9f",
      "5e665c88e0faca07f56e37a2",
      # Discount sensitive.
      "5ddd61dce0faca07d804600d", "5e665c88e0faca07f56e44f1", "60862a0887779008d0ac0aac",
      "5e74eb46f502f708cf7e9648",
      # Lost customers.
      "5e83f2bfce83c708eb635815", "5ee56bf07671f808d1dad098", "5e76937711622c08eec472bb",
      "5f05337f06f7de08db4b0008",
      # Regular customers.
      "63c72ca06c7bea19bf749fef", "607f5951124fae08c8e6ec37", "61a4020c1a46874cbbee16d8",
      "5e665c8be0faca07f56e865b"
    ),
    full_name = "", email = "", phone = ""
  ) |>
    filter(facility == store_credentials$facility) |>
    bind_rows(checked_in) |>
    # Drop real data.
    select(-full_name, -email, -phone) |>
    rename(real_customer_id = customer_id) |>
    left_join(anonymized_customers, by = "real_customer_id") |>
    select(-real_customer_id) |>
    # Drop customers not found.
    filter(!is.na(customer_id)) |>
    # Fix types.
    mutate(customer_id = as.character(customer_id))
}
