library(data.table)
library(fs)
library(parallel)

## clear log dir if exists
clear_logs <- function() {
  if (fs::dir_exists("log")) fs::dir_delete("log")
  if (fs::file_exists("cluster.log")) fs::file_delete("cluster.log")
}



clear_logs()

test_that("Testing run_leaflogix", {
  check_result <- function(x) {
    expect_named(x, c("data", "logs"), ignore.order = TRUE)
    expect_true(is.data.table(x$data))
    expect_true(is.data.table(x$logs))
  }

  res1 <- run_leaflogix(ncores = 2, test_run = TRUE)
  check_result(res1)

  res2 <- run_leaflogix(ncores = 2, org = "verano", test_run = TRUE)
  check_result(res2)

  res3 <- run_leaflogix(ncores = 1, org = "verano", store = "elizabethmed")
  check_result(res3)

  res4 <- run_leaflogix(ncores = 2, endpt = "employees", test_run = TRUE)
  check_result(res4)

  clear_logs()
})

test_that("Testing run_population", {
  check_result <- function(x) {
    expect_named(x, c("data", "logs"), ignore.order = TRUE)
    expect_true(is.data.table(x$data[[1]]$population))
    expect_true(is.data.table(x$logs))
  }

  res5 <- run_population(ncores = 2, org = "wallflower", days = 1)
  check_result(res5)

  res6 <- run_population(ncores = 1, org = "verano", store = "elizabethmed", days = 1)
  check_result(res6)

  clear_logs()
})


test_that("Testing run_stock_snapshot", {
  check_result <- function(x) {
    expect_named(x, c("data", "logs"), ignore.order = TRUE)
    expect_true(is.data.table(x$data[[1]]$stock_snapshot))
    expect_true(is.data.table(x$logs))
  }

  res7 <- run_stock_snapshot(ncores = 2, org = "wallflower", days = 2)
  check_result(res7)

  res8 <- run_stock_snapshot(ncores = 1, org = "verano", store = "elizabethmed", days = 1)
  check_result(res8)

  clear_logs()
})


