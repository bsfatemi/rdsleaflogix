
library(data.table)
library(rdleaflogix)
library(parallel)
library(stringr)
library(pins)

source("data-raw/leaflogix-schema-data.R")  ## creates object used in next line
source("data-raw/leaflogix-config-data.R")  ## creates .pos_config environment for package
source("data-raw/leaflogix-testing-data.R") ## creates .pos_data object for testing purposes

board <- board_connect(auth = "envvar")
brand_patterns <- pin_read(board, "bobbyf/brand_patterns")

usethis::use_data(.pos_data, .pos_config, brand_patterns, internal = TRUE, overwrite = TRUE)

## clean up
# fs::dir_delete("log")

# rm(list = c("res", "pl", "ll_schema", "f", ".pos_data", ".pos_config"))
# rstudioapi::restartSession()
