# Sirius App Data
# build_sirius_appdata.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  dplyr[`%>%`, distinct, left_join, mutate, select, semi_join],
  generator[r_full_names]
)

build_demo_sirius_appdata <- function(srs_appdata, polaris_appdata) {
  # Filter it down to match polaris_appdata
  srs_appdata <- semi_join(srs_appdata, distinct(polaris_appdata, customer_id), by = "customer_id")

  # STRIP OUT THE PII
  no_pii <- select(srs_appdata, -phone, -email, -full_name)

  # MASK THE SOLD BYS
  sold_bys <- distinct(no_pii, sold_by_id) %>%
    mutate(sold_by = r_full_names(n = nrow(.)))

  # CLEAN UP
  df <- left_join(no_pii, sold_bys, by = c("sold_by_id", "sold_by")) %>%
    left_join(select(
      polaris_appdata,
      customer_id,
      phone = last_customer_phone,
      full_name = last_customer_name,
      email = last_customer_email
    ), by = "customer_id") %>%
    mutate(
      dur_process = pmax(rnorm(mean = 60, n = nrow(.), sd = 10), 10),
      dur_total = dur_process * 1.1,
    )

  df$org <- "demo"

  return(df)
}
