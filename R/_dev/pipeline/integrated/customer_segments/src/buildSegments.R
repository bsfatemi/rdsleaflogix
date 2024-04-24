box::use(
  data.table[...],
  lubridate[as_date],
  stats[quantile, sd],
  hcaconfig[dbc, dbd],
  hcadatatools[dbQueryFetch],
  logr[put],
  stringr[str_detect, str_glue],
  rlang[caller_env]
)

buildSegments <- function(org_vec) {

  ..get_pop <- function(oid, cn) {
    env <- caller_env(n = 3)

    # log iteration if total and iter are available
    msg <- str_glue("INFO: <{oid}>...getting population")
    if (all(c("tot", "iter") %in% ls(env))) {
      env$iter <- env$iter + 1
      msg <- paste(msg, str_glue("({iter}/{tot})", .envir = env))
    }
    put(msg, hide_notes = TRUE)

    ## set query for pop data
    qry <- str_glue("
      SELECT org_uuid,
        order_id,
        customer_id,
        email,
        phone,
        order_time_utc,
        category3,
        order_line_total AS item_total,
        order_line_subtotal AS item_subtotal,
        order_line_discount AS item_discount
      FROM population
      WHERE org_uuid = '{oid}'
        AND order_time_utc > '2017-01-01'
        AND ((email IS NOT NULL AND email != '') OR (phone IS NOT NULL))
        AND category3 IN (
            'EDIBLES',
            'VAPES',
            'FLOWER',
            'TOPICALS',
            'TINCTURES',
            'DRINKS',
            'PREROLLS',
            'EXTRACTS',
            'TABLETS_CAPSULES'
        )
        AND order_line_subtotal < 1000
        AND order_line_subtotal > 5
    ")
    dbQueryFetch(env$cn, qry, 10^5)
  }

  ..build_segments <- function(DT) {

    ## data must be either keyed by phone or email
    if (!do.call("xor", as.list(c("phone", "email") %in% key(DT))))
      stop("pop data (arg `DT`) must be keyed by either phone or email")

    ## return if not enough contact info for org
    by <- key(DT)

    if (DT[!.(NA), .N, customer_id][, .N] < 100) {
      put(str_glue("WARN: <{DT[1, org_uuid]}>...not enough {by}s"), hide_notes = TRUE)
      return(NULL)
    }

    keyCols <- c(by, "customer_id")

    ## Summarize customer spend by day
    c_ordr <- DT[, .(
      order_tot = sum(item_subtotal, na.rm = TRUE),
      order_disc = -1 * sum(item_discount, na.rm = TRUE)
    ), keyby = .(get(by), customer_id, order_date = as_date(order_time_utc))]
    setnames(c_ordr, 1, by)

    ## keep customers with at least 3 orders and summarize by each
    c_summ <- c_ordr[c_ordr[, .N, keyby = keyCols][N > 2, !"N"]][, .(
      totalOrders = .N,
      totalSpend = sum(order_tot),
      totalDisc = sum(order_disc),
      aveDaysBetween = ceiling(as.numeric(mean(diff(order_date)), units = "days")),
      sdDaysBetween = ceiling(as.numeric(sd(diff(order_date)), units = "days")),
      aveOrderSize = mean(order_tot),
      sdOrderSize = sd(order_tot)
    ), keyby = keyCols]

    ## Upper and lower bounds for these stats are used to classify customers
    ub_order_n  <- c_summ[, quantile(totalOrders)["75%"]]
    lb_days_ave <- c_summ[, quantile(aveDaysBetween)["25%"]]
    lb_days_sd  <- c_summ[, quantile(sdDaysBetween)["25%"]]
    ub_order_sz <- c_summ[, quantile(aveOrderSize)["75%"]]

    ## Long term customers are those that have the most orders
    c_summ[, "is_long_term" := totalOrders > ub_order_n]

    ## Customers that purchase most often
    c_summ[, "is_most_frequent" := aveDaysBetween < lb_days_ave & sdDaysBetween < lb_days_sd]

    ## Customers that buy the most when they order
    c_summ[, "is_high_spender" := aveOrderSize > ub_order_sz]

    ## Calculate share of dollars spent for each category by customer
    setkeyv(DT, keyCols)
    setkeyv(c_summ, keyCols)

    tmp <- DT[
      c_summ[, keyCols, with = FALSE],
      sum(item_subtotal),
      c(keyCols, "category3")
    ][, "V2" := V1 / sum(V1), keyCols][]

    ##  Note there are only 9 categories we care about, set levels to ensure we capture all
    ##  even if this org has some categories missing
    levs <- c("EDIBLES", "VAPES", "FLOWER", "PREROLLS", "EXTRACTS", "TOPICALS",
              "DRINKS", "TINCTURES", "TABLETS_CAPSULES")
    tmp[, category3 := factor(category3, levs)]
    f <- get(by) + customer_id ~ category3
    c_summ2 <- setkeyv(setnames(dcast(tmp, f, value.var = "V2", fill = 0), 1, by), keyCols)[c_summ]

    ## set categories that do not appear in orgs data
    new_cols <- levs[!levs %in% names(c_summ2)[str_detect(names(c_summ2), "^[A-Z]+(_[A-Z]+)?$")]]
    for (col in new_cols) {
      set(c_summ2, NULL, col, 0)
    }

    ## segments defined as customers who spent at least 75% of their dollars in one category
    c_summ2[, "is_preroll_only" := PREROLLS > .75]
    c_summ2[, "is_edible_only" := EDIBLES > .75]
    c_summ2[, "is_extract_only" := EXTRACTS > .75]
    c_summ2[, "is_flower_only" := FLOWER > .75]
    c_summ2[, "is_vape_only" := VAPES > .75]

    ## Discount motivated customers
    c_summ2[, "is_disc_sensitive" :=
              totalDisc / totalSpend > quantile(totalDisc / totalSpend)["75%"]]

    OUT <- cbind(
      DT[1, .(org_uuid, segment_type = by)],
      c_summ2[, .(customer_id, contact = get(by))],
      c_summ2[, str_detect(names(c_summ2), "^is_"), with = FALSE]
    )
    return(OUT[])
  }

  cn <- dbc(Sys.getenv("HCA_ENV", "prod2"), "integrated")
  on.exit(dbd(cn))

  iter <- 0
  tot <- length(org_vec)
  OUT <- rbindlist(lapply(org_vec, function(oid) {

    # get pop and return if no data
    pop <- ..get_pop(oid, cn)
    if (nrow(pop) == 0) {
      put(str_glue("WARN: <{oid}>...no population data"), hide_notes = TRUE)
      return(NULL)
    }
    ## build segments if no error, else log and return null
    put(str_glue("INFO: <{oid}>...building segments"), hide_notes = TRUE)
    RES <- tryCatch({
      rbindlist(list(
        ..build_segments(setkeyv(pop, "phone")[!.(NA)]),
        ..build_segments(setkeyv(pop, "email")[!.(NA)])
      ))
    },
    error = function(c) {
      put(str_glue("ERROR: <{oid}>...segmentation failed"), hide_notes = TRUE)
      put(str_glue("DETAIL: {c$message}"), hide_notes = TRUE)
      return(NULL)
    })

    ## if successful, log a summary of the data
    if (!is.null(RES)) {
      put(str_glue("DATA: <{oid}>"), hide_notes = TRUE)
      summ <- melt(RES, measure.vars = patterns("^is"))[, .(
        in_segment = sum(value),
        not_in_segment = sum(!value)
      ), keyby = .(segment = variable, segment_type)]
      put(summ, hide_notes = TRUE)
    }
    return(RES)
  }))
  return(OUT)
}
