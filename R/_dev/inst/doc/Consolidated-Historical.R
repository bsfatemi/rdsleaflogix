## ---- include = FALSE---------------------------------------------------------
knitr::opts_chunk$set(
  collapse = TRUE,
  comment = "#>",
  eval = FALSE
)

## ----setup--------------------------------------------------------------------
#  library(rdleaflogix)
#  library(rdtools)
#  library(parallel)
#  library(data.table)

## ----get_inputs---------------------------------------------------------------
#  ## bind together, remove endpoint column and keep only org, store, auth, consumerkeys
#  argll <- rbindlist(get_pipeline_args("leaflogix"))[, tab := NULL][, unique(.SD)] |>
#    split_by_row()

## ----prep_cluster-------------------------------------------------------------
#  log_inf("...Beginning parallel run")
#
#  # make cluster with 12 nodes
#  cl <- rdleaflogix:::make_cluster(12)
#
#  # set job id for this run
#  jobId <- as.integer(Sys.time())
#  log_inf(paste0("...Init cluster with JobId ", jobId))
#
#  ## Prepare environment
#  log_inf("...Preparing environment on each node")
#
#  # export job id
#  parallel::clusterExport(cl, "jobId")
#
#  # export functions that are run by nodes
#  env <- rlang::pkg_env("rdleaflogix")
#  vars <- c("get_ll_brands", "get_ll_categories",
#            "ext_ll_brands", "ext_ll_categories",
#            "bld_ll_brand_index", "bld_ll_category_index",
#            "wrt_ll_brand_index", "wrt_ll_category_index",
#            "extract_index")
#  parallel::clusterExport(cl, vars, env)
#
#  # open log files from each node
#  log_paths <- parallel::clusterEvalQ(cl, {
#    lf <- paste0("pid-", Sys.getpid())
#    rdtools::open_log(lf, jobId)
#  })

## ----exec_brands--------------------------------------------------------------
#  fx_brands <- function(x) {
#    do.call(get_ll_brands, x) |>
#      ext_ll_brands(x$org, x$store) |>
#      extract_index("brands") |>
#      bld_ll_brand_index(x$org, x$store) |>
#      wrt_ll_brand_index(x$org, x$store)
#  }
#  parallel::parLapply(cl, argll, fx_brands)

## ----exec_categories----------------------------------------------------------
#  fx_categories <- function(x) {
#    do.call(get_ll_categories, x) |>
#      ext_ll_categories(x$org, x$store) |>
#      extract_index("categories") |>
#      bld_ll_category_index(x$org, x$store) |>
#      wrt_ll_category_index(x$org, x$store)
#  }
#  parallel::parLapply(cl, argll, fx_categories)

## ----cleanup------------------------------------------------------------------
#  # stop cluster and collect logs
#  logsDT <- rdleaflogix:::clean_cluster(cl)
#
#  if (fs::file_exists("cluster.log"))
#    fs::file_delete("cluster.log")
#
#  if (fs::dir_exists("log"))
#    fs::dir_delete("log")

