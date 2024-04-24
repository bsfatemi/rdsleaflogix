# Data sanity check for yesterday (compared to previous 30 days).
hcainternalreports::data_sanity_report(
  check_date = lubridate::today() - lubridate::days(1), previous_days = 30
)

# Consolidated tables sanity report (checks that columns are present and with correct type).
hcainternalreports::consolidated_sanity_report()
