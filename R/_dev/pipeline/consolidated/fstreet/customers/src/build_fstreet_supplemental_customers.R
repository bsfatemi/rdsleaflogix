# adds customers that are not already in the POS but from an approved file
build_fstreet_supplemental_customers <- function(trz_customers, supplemental_customers) {
  supplemental_customers$opt_out <- "FALSE"
  supplemental_customers <- supplemental_customers %>%
    rename("last_name" = "Last Name", "first_name" = "First Name", "phone" = "Phone Number")
  supplemental_customers <- supplemental_customers %>%
    group_by(phone) %>%
    mutate(customer_id = ids::random_id(bytes = 5))
  supplemental_customers <- select(
    supplemental_customers, customer_id, first_name, last_name, phone, opt_out, run_date_utc
  )

  trz_customers <- bind_rows(trz_customers, supplemental_customers)
  return(trz_customers)
}
