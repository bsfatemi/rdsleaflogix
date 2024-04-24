# Collect Telnyx Webhooks
# run.R
#
# (C) Happy Cabbage Analytics 2021

# QUERY
json <- hcahooks::get_webhooks(query_time = lubridate::now("UTC") - lubridate::minutes(6))

# STORE
pipelinetools::storeExternalRaw(json, "webhook", "happycabbage", "telnyx_webhooks")
