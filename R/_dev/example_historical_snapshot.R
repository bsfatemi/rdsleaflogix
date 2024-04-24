library(rdleaflogix)
library(data.table)
library(rdtools)
library(parallel)
library(rlang)
library(fs)




res <- run_pl_stock_snapshot(ncores = 24, n = 1, write = FALSE)

