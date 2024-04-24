
# VARS --------------------------------------------------------------------
org <- "clinicaverde"
js <- pipelinetools::rd_raw_archive("consumers", paste0(org, "_all"), "akerna")

# RUN -------------------------------------------------------------------------
consumers <- hcaakerna::extract_akerna_consumers(js)

groups <- consumers$groups
groups$run_date_utc <- lubridate::now("UTC")

group_types <- consumers$group_types
group_types$run_date_utc <- lubridate::now("UTC")

tags <- consumers$tags
tags$run_date_utc <- lubridate::now("UTC")

addresses <- consumers$addresses
addresses$run_date_utc <- lubridate::now("UTC")

ids <- consumers$ids
ids$run_date_utc <- lubridate::now("UTC")

caregivers <- consumers$caregivers
caregivers$run_date_utc <- lubridate::now("UTC")

consumers <- consumers$consumers
consumers$run_date_utc <- lubridate::now("UTC")
# ARCHIVE -----------------------------------------------------------------

## Remove List Cols
consumers$addresses <- NULL
consumers$tags <- NULL
consumers$ids <- NULL
consumers$caregivers <- NULL
caregivers$addresses <- NULL
tags$pivot <- NULL

pg <- hcaconfig::dbc("prod2", "cabbage_patch")
ex <- DBI::dbListFields(pg, paste0(org, "_all_akerna_consumers"))
consumers <- pipelinetools::check_cols(consumers, ex)
DBI::dbWriteTable(pg, paste0(org, "_all_akerna_consumers"), consumers, append = TRUE)

ex <- DBI::dbListFields(pg, paste0(org, "_all_akerna_consumer_groups"))
groups <- pipelinetools::check_cols(groups, ex)
DBI::dbWriteTable(pg, paste0(org, "_all_akerna_consumer_groups"), groups, append = TRUE)

ex <- DBI::dbListFields(pg, paste0(org, "_all_akerna_consumer_group_types"))
group_types <- pipelinetools::check_cols(group_types, ex)
DBI::dbWriteTable(pg, paste0(org, "_all_akerna_consumer_group_types"), group_types, append = TRUE)

ex <- DBI::dbListFields(pg, paste0(org, "_all_akerna_consumer_tags"))
tags <- pipelinetools::check_cols(tags, ex)
DBI::dbWriteTable(pg, paste0(org, "_all_akerna_consumer_tags"), tags, append = TRUE)

ex <- DBI::dbListFields(pg, paste0(org, "_all_akerna_consumers_addresses"))
addresses <- pipelinetools::check_cols(addresses, ex)
DBI::dbWriteTable(pg, paste0(org, "_all_akerna_consumers_addresses"), addresses, append = TRUE)

ex <- DBI::dbListFields(pg, paste0(org, "_all_akerna_consumers_ids"))
ids <- pipelinetools::check_cols(ids, ex)
DBI::dbWriteTable(pg, paste0(org, "_all_akerna_consumers_ids"), ids, append = TRUE)

ex <- DBI::dbListFields(pg, paste0(org, "_all_akerna_caregivers"))
caregivers <- pipelinetools::check_cols(caregivers, ex)
DBI::dbWriteTable(pg, paste0(org, "_all_akerna_caregivers"), caregivers, append = TRUE)

hcaconfig::dbd(pg)
