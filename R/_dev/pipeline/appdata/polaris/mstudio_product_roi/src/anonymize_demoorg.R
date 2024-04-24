box::use(
  dplyr[filter, left_join, mutate, pull, select],
  janeaustenr[austen_books],
  stringi[stri_reverse],
  stringr[str_to_sentence]
)

# Anonymize Polaris appdata.
anonymize_demoorg <- function(demoorg_data, new_org_name, demoorg_campaigns) {
  demo_sample <- austen_books() |>
    filter(text != "", nchar(text) > 50) |>
    pull(text) |>
    str_to_sentence()
  # Attempt to give the same `body` to each campaign, throughout every demoorg pipeline.
  set.seed(420)
  demoorg_campaigns <- mutate(
    demoorg_campaigns,
    campaign_id = stri_reverse(campaign_id),
    body = paste(
      sample(demo_sample, size = nrow(demoorg_campaigns), replace = TRUE),
      sample(demo_sample, size = nrow(demoorg_campaigns), replace = TRUE)
    )
  ) |>
    select(campaign_id, body)
  select(demoorg_data, -body) |>
    mutate(campaign_id = stri_reverse(campaign_id)) |>
    left_join(demoorg_campaigns, by = "campaign_id") |>
    mutate(
      product_name = gsub("mmd", "House Brand", product_name, ignore.case = TRUE),
      brand_name = gsub("mmd", "House Brand", brand_name, ignore.case = TRUE),
      org = new_org_name
    )
}
