#' Functions to Run Leaflogix Pipeliens
#'
#' @param ncores if not specified will execute with available cores on system
#' @param org org short name
#' @param store store short name
#' @param days number of days to build data for
#' @param auth internal - advanced use only
#' @param consumerkey internal - advanced use only
#' @param write wether to land data in integrated database. Default FALSE
#' @param cl cluster object
#' @param export_vars names of variables in the parent environment to export to nodes in
#' the cluster
#' @param .env optional environment to pass in that contains objects named in export_vars
#' @param input_list a list of inputs to distribute across the cluster and run \code{fun}
#' @param fun function to execute in parallel
#' @param pkgs package name(s) to export to cluster
#' @param LDT internal - advanced use only
#' @param stop if TRUE (default) also close the cluster
#' @param LB run with load balancing (default is FALSE)
#' @param endpt api endpoint name
#' @param stop_cluster whether to stop the cluster after run (default is TRUE)
#' @param test_run run with a sample of 10 inputs
#' @param outfile log file path for cluster process
#'
#' @import data.table
#' @importFrom rdtools log_suc log_wrn log_err open_log close_log log_inf read_logs
#' @importFrom stringr str_glue str_squish str_subset
#' @importFrom parallel detectCores makeCluster stopCluster clusterExport clusterEvalQ parLapply
#' @importFrom fs path dir_ls file_delete file_exists
#' @importFrom rlang current_env
#' @importFrom utils lsf.str
#' @importFrom stats setNames
#'
#' @name leaflogix-run
NULL



#' @describeIn leaflogix-run build population subset for a specific individual location
#' @export
run_population <- function(ncores = NULL,
                           org = NULL,
                           store = NULL,
                           days = 1,
                           write = FALSE) {

  ## Set cores if not specified
  ncores[is.null(ncores)] <- as.numeric(Sys.getenv("N_CORES", parallel::detectCores()))

  ## Get args to iterate with
  argsDT <- get_leaflogix_index(org, store)[, "days" := days][, "write" := write]
  argsll <- setNames(split_by_row(argsDT), argsDT[, paste0(org, ".", store)])

  ## Function to iterate with
  fx <- function(x) pl_population(x$org, x$store, x$auth, x$consumerkey, x$days, x$write)

  ## Run and return
  pl_runner(ncores, argsll, fx, LB = days > 30)
}


#' @describeIn leaflogix-run Primary run function for building and landing stock_snapshot
#' @export
run_stock_snapshot <- function(ncores = NULL,
                               org = NULL,
                               store = NULL,
                               days = 1,
                               write = FALSE) {

  ## Set cores if not specified
  ncores[is.null(ncores)] <- as.numeric(Sys.getenv("N_CORES", parallel::detectCores()))

  ## Function to iterate with
  fx <- function(x) pl_stock_snapshot(x$org, x$store, x$auth, x$consumerkey, x$days, x$write)

  ## Get args to iterate with
  argsDT <- get_leaflogix_index(org, store)[, "days" := days][, "write" := write]
  argsll <- setNames(split_by_row(argsDT), argsDT[, paste0(org, ".", store)])

  ## Run and return
  pl_runner(ncores, argsll, fx, LB = days > 30)
}


#' @describeIn leaflogix-run Primary run function for leaflogix orgs
#' @export
run_leaflogix <- function(ncores = NULL,
                          org = NULL,
                          store = NULL,
                          endpt = NULL,
                          test_run = FALSE) {


  ## Get arguments
  argsll <- get_leaflogix_args(org, store, endpt, test_run)

  ## Set cores
  ncores[is.null(ncores)] <- as.numeric(Sys.getenv("N_CORES", parallel::detectCores()))

  ## Define function to execute on cluster nodes
  fx <- function(x) pl_leaflogix(x$org, x$store, x$auth, x$consumerkey, x$tab)

  ## Execute and transform results
  res <- pl_runner(ncores, argsll, fx, stop_cluster = FALSE)

  ## This groups the results together by org and store and then collapses tables into
  ## an index of datasets
  res_index <- sapply(unique(names(res$data)), function(os) {
    ll <- unlist(res$data[which(names(res$data) == os)], recursive = FALSE)
    names(ll) <- stringr::str_remove(names(ll), paste0(os, "."))
    loc <- stringr::str_split(os, "\\.", 2, simplify = TRUE)
    setkey(extract_index(ll, loc[1, 1], loc[1, 2]), name)[]
  }, simplify = FALSE)
  log_suc("...Consolidated pipeline complete")

  ## Build population
  log_inf("...Starting population build")

  ## filter on population inputs and run build
  fx2 <- function(indx) {
    x <- indx[c("customer_summary", "product_summary", "order_items", "order_summary")]
    bld_ll_population(x, org = x[1, org], store = x[1, store])
  }

  ## Execute
  pop <- pl_runner(ncores, res_index, fx2, stop_cluster = TRUE, LB = TRUE)

  ## Morph results
  pop_index <- sapply(names(pop$data), function(os) {
    org <- stringr::str_extract(os, ".+(?=\\.)")
    store <- stringr::str_extract(os, "(?<=\\.).+")
    extract_index(pop$data[[os]], org, store)
  }, simplify = FALSE)
  log_suc("...Population build complete")

  ## Create output object
  OUT <- list(
    data = setkey(rbindlist(list(
      rbindlist(pop_index),
      rbindlist(res_index)
    )), org, store, name)[],
    logs = rbindlist(list(res$logs, pop$logs), fill = TRUE)
  )
  return(OUT)
}

#' @describeIn leaflogix-run execution function
pl_runner <- function(ncores, input_list, fun, stop_cluster = TRUE, LB = FALSE) {
  if (ncores == 1) {
    open_log(paste0("pid-", Sys.getpid()), as.integer(Sys.time()))
    ll <- sapply(input_list, fun, simplify = FALSE)
    DT <- close_log()
    OUT <- list(data = ll, logs = DT)

  } else {
    ## run in parallel.. get default cluster
    cl <- make_cluster(ncores) |>
      init_cluster()

    ll <- exec_cluster(cl, input_list, fun, LB = LB)
    DT <- close_cluster(cl, stop_cluster)
    OUT <- list(data = ll, logs = DT)
  }

  cl <- parallel::getDefaultCluster()
  if (!is.null(cl))
    parallel::stopCluster(cl)

  return(OUT)
}


#' @describeIn leaflogix-run build population subset for a specific individual location
pl_population <- function(org, store, auth, consumerkey, days = 1, write = FALSE) {
  ll <- get_ll_population(org, store, auth, consumerkey, n = days) |>
    ext_ll_population(org, store) |>
    bld_ll_population(org, store)

  if (is.null(ll$population)) {
    log_err("...Aborting population build for location")
    return(ll)
  }

  ## Write population if specified
  if (write)
    wrt_ll_population(ll, org, store)
  return(ll)
}


#' @describeIn leaflogix-run build population subset for a specific individual location
pl_stock_snapshot <- function(org, store, auth, consumerkey, days = 1, write = FALSE) {
  ll <- get_ll_stocksnapshot(org, store, auth, consumerkey, n = days) |>
    ext_ll_stocksnapshot(org, store) |>
    bld_ll_stocksnapshot(org, store)

  if (is.null(ll$stock_snapshot)) {
    log_err("...Aborting population build for location")
    return(ll)
  }

  if (write)
    wrt_ll_stocksnapshot(ll, org, store)
  return(ll)
}


#' @describeIn leaflogix-run execution orchestrated by \code{run_leaflogix}
pl_leaflogix <- function(org, store, auth, consumerkey, endpt) {
  ## lookup the get/extract functions we need for this endpoint
  ..get <- get(stringr::str_glue("get_ll_{endpt}"))
  ..ext <- get(stringr::str_glue("ext_ll_{endpt}"))
  ..bld <- get(stringr::str_glue("bld_ll_{endpt}"))

  ## run the get extract processes and collapse results to index
  res <- ..get(org, store, auth, consumerkey) |>
    ..ext(org, store) |>
    ..bld(org, store)
  return(res)
}


#' @describeIn leaflogix-run make cluster create cluster.log file
#' @export
make_cluster <- function(ncores, outfile = "cluster.log") {
  log_inf("...Preparing for parallel execution")

  if (parallel::detectCores() < ncores) {
    log_wrn("...using max number of cores available")
    ncores <- parallel::detectCores()
  }

  cl <- parallel::getDefaultCluster()

  if (fs::file_exists(outfile)) {
    fs::file_delete(outfile)
    fs::file_create(outfile)
  }

  if (is.null(cl)) {
    log_inf(paste0("...Creating cluster with ", ncores, " nodes"))
    cl <- parallel::makeCluster(ncores, outfile = outfile)
    parallel::setDefaultCluster(cl)
  } else {
    log_inf("...Using default cluster")
  }
  return(invisible(cl))
}

#' @describeIn leaflogix-run initialize cluster environment
#' @export
init_cluster <- function(cl = NULL, export_vars = NULL, .env = NULL, pkgs = NULL) {
  if (is.null(cl)) {
    cl <- parallel::getDefaultCluster()
    if (is.null(cl)) {
      stop("Default cluster not set and none provided in args", call. = FALSE)
    }
  }
  jobId <- as.integer(Sys.time())
  log_inf(paste0("...Init cluster with JobId ", jobId))

  curr_env <- rlang::current_env()
  parallel::clusterExport(cl, "jobId", curr_env)

  ## Prepare environment
  log_inf("...Preparing environment on each node")

  if (!is.null(export_vars)) {
    if (is.null(.env))
      .env <- rlang::env_parent()
    parallel::clusterExport(cl, export_vars, .env)
  }

  ## Load packages on nodes in cluster
  pkgs <- unique(c("rdleaflogix", "rdtools", pkgs))
  for (p in pkgs) {
    parallel::clusterExport(
      cl = cl,
      unclass(lsf.str(envir = asNamespace(p), all = TRUE)),
      envir = as.environment(asNamespace(p))
    )
  }

  ## Open log files on nodes in cluster
  parallel::clusterEvalQ(cl, {
    open_log(paste0("pid-", Sys.getpid()), jobId)
  })

  ## show working directory and log directory paths
  log_inf(paste0("...Working directory: ", getwd()))
  log_inf(paste0("...Log Directory: ", fs::path("log", jobId)))

  invisible(cl)
}

#' @describeIn leaflogix-run clean up environment, gather and return log files, stop cluster
#' @export
close_cluster <- function(cl = NULL, stop = TRUE) {
  if (is.null(cl)) {
    cl <- parallel::getDefaultCluster()
    if (is.null(cl)) {
      log_wrn("No default cluster found to close")
      return(NULL)
    }
  }

  if (stop) {
    on.exit({
      tryCatch(
        parallel::stopCluster(cl),
        error = function(c) log_inf("...Failed attempting to stop cluster"),
        finally = log_suc("...Stopped Cluster")
      )
    })
  }

  ## Clean cluster
  FAILED <- FALSE
  logs <- tryCatch(
    parallel::clusterEvalQ(cl, close_log()),
    error = function(c) FAILED <<- TRUE,
    finally = {
      if (FAILED) {
        log_err("...Failed collecting logs from nodes")
      } else {
        log_suc("...Collected logs from nodes")
      }
    })

  if (!FAILED)
    return( rbindlist(logs, fill = TRUE) )
  return(NULL)
}

#' @describeIn leaflogix-run exec on cluster
#' @export
exec_cluster <- function(cl = NULL, input_list, fun, LB = FALSE) {
  RUN_SEQ <- FALSE
  if (is.null(cl)) {
    cl <- parallel::getDefaultCluster()
    if (is.null(cl)) {
      log_wrn("...No default cluster set and none provided")
      RUN_SEQ <- TRUE
    }
  }
  if (RUN_SEQ) {
    log_inf("...Falling back to non-parallel execution")
    lapply(input_list, fun) |>
      setNames(names(input_list))
  } else {
    log_inf("...Beginning parallel run")
    if (LB) {
      parallel::parLapplyLB(cl, input_list, fun) |>
        setNames(names(input_list))
    } else {
      parallel::parLapply(cl, input_list, fun) |>
        setNames(names(input_list))
    }
  }



}


#' @describeIn leaflogix-run Summarize log errors returned by \code{run_pl_leaflogix}
summarize_logs <- function(LDT) {
  LDT <- copy(LDT)
  LDT[, TimestampUTC := lubridate::as_datetime(TimestampUTC)]
  LDT[, Detail := factor(Detail,
                         levels = c("GET", "EXT", "BLD"),
                         labels = c("Get", "Extract", "Build"))]
  LDT[, Level := factor(Level,
                        levels = c("SUCCESS", "ERROR", "WARNING", "INFO"),
                        labels = c("Success", "Error", "Warning", "Info"))]
  setkey(LDT, Level)

  eventLevels <- dcast(
    LDT[, .N, .(
      Level,
      POS = Message,
      Process = Detail,
      Data = Detail.1
    )],
    Level + POS ~ Process, sum,
    drop = FALSE,
    value.var = "N"
  )

  getLevels <- dcast(
    LDT[Detail == "Get", .N, .(
      Level,
      POS = Message,
      Process = Detail,
      Data = Detail.1
    )],
    Process + POS + Data ~ Level,
    sum,
    drop = c(TRUE, FALSE),
    value.var = "N"
  )

  extLevels <- dcast(
    LDT[Detail == "Extract", .N, .(
      Level,
      POS = Message,
      Process = Detail,
      Data = Detail.1
    )],
    Process + POS + Data ~ Level,
    sum,
    drop = c(TRUE, FALSE),
    value.var = "N"
  )

  bldLevels <- dcast(
    LDT[!"Info"][Detail == "Build", .N, .(
      Level,
      Process = Detail,
      POS = Message,
      Data = Detail.1
    )][, Data := stringr::str_remove(Data, " (?=\\().+(?<=\\))")][],
    Process + POS + Data ~ Level,
    sum,
    drop = c(TRUE, FALSE),
    value.var = "N"
  )

  all_errors <- LDT[Level == "Error"]

  OUT <- list(eventLevels, getLevels, extLevels, bldLevels, all_errors)
  return(OUT)
}


#' @describeIn leaflogix-run Summarize log errors returned by \code{run_pl_leaflogix}
summarize_fail <- function(LDT) {
  LDT <- copy(LDT)
  LDT[, Detail := factor(Detail,
                         levels = c("GET", "EXT", "BLD"),
                         labels = c("Get", "Extract", "Build"))]
  LDT[, Level := factor(Level,
                        levels = c("SUCCESS", "ERROR", "WARNING", "INFO"),
                        labels = c("Success", "Error", "Warning", "Info"))]

  setnames(LDT,
           c("Message", "Detail", "Detail.1", "Detail.2", "Detail.3"),
           c("POS", "Process", "Data", "Location", "Detail"))


  LDT[, c("Org", "Store") := as.data.table(
    stringr::str_split(Location, "\\.", n = 2, simplify = TRUE)
  )][, Location := NULL]

  LDT[, Date := lubridate::as_date(TimestampUTC)]

  LDT[stringr::str_detect(Data, "^\\/"), DataType := "Endpoint"]
  LDT[stringr::str_detect(Data, "_"), DataType := "Table"]

  LDT[DataType == "Endpoint", Data := stringr::str_remove(Data, "...20.+$")]

  ioll <- lookupIO_All()
  endpt_table_map <- rbindlist(lapply(
    LDT[DataType == "Endpoint", .N, Data][, Data], function(ep) {
    id <- names(which(sapply(ioll, function(x) x$endpoint == ep)))
    tmp <- ioll[[id]]
    endpt <- tmp$endpoint
    dbtables <- names(tmp[-1])
    rbindlist(lapply(dbtables, function(tb) {
      data.table(api_endpoint = endpt, extracted_table = tmp[[tb]], db_table = tb)
    }))
  }))

  OUT <- rbindlist(apply(endpt_table_map, 1, function(row) {
    ep <- row[1]
    ext <- row[2]
    db <- row[3]

    tmpLDT <- LDT[Level %in% c("Success", "Error")]
    res <- rbindlist(lapply(c(ep, db), function(x) {
      tmpLDT[Data == x, .(
        Level,
        POS,
        Process,
        Endpoint = ep,
        Extracted = ext,
        DbTable = db,
        Org,
        Store
      )]
    }))

    dcast(
      res,
      POS + Org + Store + Endpoint + Extracted + DbTable ~ Process,
      value.var = "Level"
    )[Get != "Success" | Extract != "Success" | Build != "Success"]
  }))[order(Org, Store, Endpoint, Extracted, DbTable)]

  OUT[Get == "Success", Get_Success := 1]
  OUT[Get == "Error", Get_Success := 0]
  OUT[Extract == "Success", Extract_Success := 1]
  OUT[Extract == "Error", Extract_Success := 0]
  OUT[Build == "Error" | is.na(Build), Build_Success := 0]
  OUT[, c("Get", "Extract", "Build") := NULL]
  setnames(OUT,
           c("Get_Success", "Extract_Success", "Build_Success"), c("Get", "Extract", "Build"))
  return(OUT[])
}


