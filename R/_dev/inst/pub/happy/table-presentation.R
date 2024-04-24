library(gt)
library(lubridate)
library(data.table)

sf <- function(x) format(x, format = "%H:%M:%S %P on %b %d, %Y")
report_start <- sf(now())

happy_gray_1 <- "#ededed"
happy_gray_2 <- "#dddddc"
happy_gray_3 <- "#c8c8c8"

happy_blue_1 <- "#c5e7fd"
happy_blue_2 <- "#85c5fc"
happy_blue_3 <- "#6fa5d4"

happy_green_1 <- "#afeca4"
happy_green_2 <- "#69cf48"
happy_green_3 <- "#72994e"

happy_red_1 <- "#f0998a"
happy_red_2 <- "#e35148"
happy_red_3 <- "#c93227"

happy_gold_1 <- "#ffe94d"
happy_gold_2 <- "#ebc147"
happy_gold_3 <- "#d4a219"


happy_colors_1 <- colorRampPalette(colors = c(
  happy_green_1, happy_red_1, happy_gold_1, happy_blue_1
))(4)

happy_colors_2 <- colorRampPalette(colors = c(
  happy_green_2, happy_red_2, happy_gold_2, happy_blue_2
))(4)

happy_colors_3 <- colorRampPalette(colors = c(
  happy_green_3, happy_red_3, happy_gold_3, happy_blue_3
))(4)

showEventCounts <- function(DT) {
  n_rows <- nrow(DT)

  get_row_col <- function(data, row) {
    data_color(
      data,
      rows = row,
      columns = Get,
      palette = happy_colors_1[row]
    )
  }

  ext_row_col <- function(data, row) {
    data_color(
      data,
      rows = row,
      columns = Extract,
      palette = happy_colors_2[row]
    )
  }

  bld_row_col <- function(data, row) {
    data_color(
      data,
      rows = row,
      columns = Build,
      palette = happy_colors_3[row]
    )
  }

  # Create gt table
  DT <- DT |>
    gt() |>
    cols_label(
      Get = md("**Get**"),
      Extract = md("**Extract**"),
      Build = md("**Build**")
    )

  DT |>
    get_row_col(1) |>
    get_row_col(2) |>
    get_row_col(3) |>
    get_row_col(4) |>
    ext_row_col(1) |>
    ext_row_col(2) |>
    ext_row_col(3) |>
    ext_row_col(4) |>
    bld_row_col(1) |>
    bld_row_col(2) |>
    bld_row_col(3) |>
    bld_row_col(4)
}

showEventDetails <- function(DT) {
  all_index_1 <- DT[get("Process") == "Get", which = TRUE]
  all_index_2 <- DT[get("Process") == "Extract", which = TRUE]
  all_index_3 <- DT[get("Process") == "Build", which = TRUE]

  errors_index_1  <- DT[get("Process") == "Get" & get("Error") > 0, which = TRUE]
  success_index_1 <- DT[get("Process") == "Get" & get("Success") > 0, which = TRUE]
  warning_index_1 <- DT[get("Process") == "Get" & get("Warning") > 0, which = TRUE]
  info_index_1    <- DT[get("Process") == "Get" & get("Info") > 0, which = TRUE]

  errors_index_2  <- DT[get("Process") == "Extract" & get("Error") > 0, which = TRUE]
  success_index_2 <- DT[get("Process") == "Extract" & get("Success") > 0, which = TRUE]
  warning_index_2 <- DT[get("Process") == "Extract" & get("Warning") > 0, which = TRUE]
  info_index_2    <- DT[get("Process") == "Extract" & get("Info") > 0, which = TRUE]

  errors_index_3  <- DT[get("Process") == "Build" & get("Error") > 0, which = TRUE]
  success_index_3 <- DT[get("Process") == "Build" & get("Success") > 0, which = TRUE]
  warning_index_3 <- DT[get("Process") == "Build" & get("Warning") > 0, which = TRUE]
  info_index_3    <- DT[get("Process") == "Build" & get("Info") > 0, which = TRUE]

  setnames(DT, "Data", "Endpoint or Table")

  DT <- DT |>
    gt() |>
    cols_label(
      Process = md("**Process**"),
      POS = md("**POS**"),
      `Endpoint or Table` = md("**Endpoint or Table**"),
      Success = md("**Success**"),
      Error = md("**Error**"),
      Warning = md("**Warning**"),
      Info = md("**Info**")
    )



  ##
  ## Color 0s in Gray for ERRORS and...
  ##

  ## GET
  gray_index_1 <- all_index_1[which(!all_index_1 %in% errors_index_1)]
  if (length(gray_index_1) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_1,
        columns = Error,
        palette = happy_gray_1
      )
  }

  ## EXTRACT
  gray_index_2 <- all_index_2[which(!all_index_2 %in% errors_index_2)]
  if (length(gray_index_2) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_2,
        columns = Error,
        palette = happy_gray_2
      )
  }

  ## BUILD
  gray_index_3 <- all_index_3[which(!all_index_3 %in% errors_index_3)]
  if (length(gray_index_3) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_3,
        columns = Error,
        palette = happy_gray_3
      )
  }

  ##
  ## Color 0s in Gray for SUCCESS and...
  ##

  ## GET
  gray_index_1 <- all_index_1[which(!all_index_1 %in% success_index_1)]
  if (length(gray_index_1) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_1,
        columns = Success,
        palette = happy_gray_1
      )
  }

  ## EXTRACT
  gray_index_2 <- all_index_2[which(!all_index_2 %in% success_index_2)]
  if (length(gray_index_2) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_2,
        columns = Success,
        palette = happy_gray_2
      )
  }

  ## BUILD
  gray_index_2 <- all_index_3[which(!all_index_3 %in% success_index_3)]
  if (length(gray_index_3) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_3,
        columns = Success,
        palette = happy_gray_3
      )
  }

  ##
  ## Color 0s in Gray for WARNING and...
  ##

  ## GET
  gray_index_1 <- all_index_1[which(!all_index_1 %in% warning_index_1)]
  if (length(gray_index_1) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_1,
        columns = Warning,
        palette = happy_gray_1
      )
  }

  ## EXTRACT
  gray_index_2 <- all_index_2[which(!all_index_2 %in% warning_index_2)]
  if (length(gray_index_2) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_2,
        columns = Warning,
        palette = happy_gray_2
      )
  }

  ## BUILD
  gray_index_3 <- all_index_3[which(!all_index_3 %in% warning_index_3)]
  if (length(gray_index_3) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_3,
        columns = Warning,
        palette = happy_gray_3
      )
  }


  ##
  ## Color 0s in Gray for INFO and...
  ##

  ## GET
  gray_index_1 <- all_index_1[which(!all_index_1 %in% info_index_1)]
  if (length(gray_index_1) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_1,
        columns = Info,
        palette = happy_gray_1
      )
  }

  ## EXTRACT
  gray_index_2 <- all_index_2[which(!all_index_2 %in% info_index_2)]
  if (length(gray_index_2) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_2,
        columns = Info,
        palette = happy_gray_2
      )
  }


  ## BUILD
  gray_index_3 <- all_index_3[which(!all_index_3 %in% info_index_3)]
  if (length(gray_index_3) > 0) {
    DT <- DT |>
      data_color(
        rows = gray_index_3,
        columns = Info,
        palette = happy_gray_3
      )
  }

  ##
  ## Color in Non 0s for GET
  ##
  if (length(errors_index_1) > 0) {
    DT <- DT |>
      data_color(
        rows = errors_index_1,
        columns = Error,
        palette = happy_red_1
      )
  }

  if (length(success_index_1) > 0) {
    DT <- DT |>
      data_color(
        rows = success_index_1,
        columns = Success,
        palette = happy_green_1
      )
  }

  if (length(warning_index_1) > 0) {
    DT <- DT |>
      data_color(
        rows = warning_index_1,
        columns = Warning,
        palette = happy_gold_1
      )
  }

  if (length(info_index_1) > 0) {
    DT <- DT |>
      data_color(
        rows = info_index_1,
        columns = Info,
        palette = happy_blue_1
      )
  }


  ##
  ## Color in Non 0s for EXTRACT
  ##
  if (length(errors_index_2) > 0) {
    DT <- DT |>
      data_color(
        rows = errors_index_2,
        columns = Error,
        palette = happy_red_2
      )
  }

  if (length(success_index_2) > 0) {
    DT <- DT |>
      data_color(
        rows = success_index_2,
        columns = Success,
        palette = happy_green_2
      )
  }

  if (length(warning_index_2) > 0) {
    DT <- DT |>
      data_color(
        rows = warning_index_2,
        columns = Warning,
        palette = happy_gold_2
      )
  }

  if (length(info_index_2) > 0) {
    DT <- DT |>
      data_color(
        rows = info_index_2,
        columns = Info,
        palette = happy_blue_2
      )
  }


  ##
  ## Color in Non 0s for BUILD
  ##
  if (length(errors_index_3) > 0) {
    DT <- DT |>
      data_color(
        rows = errors_index_3,
        columns = Error,
        palette = happy_red_3
      )
  }

  if (length(success_index_3) > 0) {
    DT <- DT |>
      data_color(
        rows = success_index_3,
        columns = Success,
        palette = happy_green_3
      )
  }

  if (length(warning_index_3) > 0) {
    DT <- DT |>
      data_color(
        rows = warning_index_3,
        columns = Warning,
        palette = happy_gold_3
      )
  }

  if (length(info_index_3) > 0) {
    DT <- DT |>
      data_color(
        rows = info_index_3,
        columns = Info,
        palette = happy_blue_3
      )
  }
  DT
}

showErrorDetails <- function(DT) {
  if (is.null(DT$Get))
    return(NULL)

  DT <- copy(DT)
  get_succ_index <- DT[get("Get") == 1, which = TRUE]
  get_fail_index <- DT[get("Get") == 0, which = TRUE]

  ext_succ_index <- DT[get("Extract") == 1, which = TRUE]
  ext_fail_index <- DT[get("Extract") == 0, which = TRUE]

  bld_succ_index <- DT[get("Build") == 1, which = TRUE]
  bld_fail_index <- DT[get("Build") == 0, which = TRUE]


  setnames(DT,
           c("Endpoint", "Extracted", "DbTable"),
           c("Get Endpoint", "Extract Data", "Build Table"),
           skip_absent = TRUE)

  DT[, c("Get", "Extract", "Build") := NULL]


  tmp <- DT |>
    gt() |>
    cols_label(
      POS = md("**POS**"),
      Org = md("**Org**"),
      Store = md("**Store**"),
      `Get Endpoint` = md("**Get Endpoint**"),
      `Extract Data` = md("**Extract Data**"),
      `Build Table` = md("**Build Table**")
    )

  if (length(get_succ_index) > 0) {
    tmp <- tmp |>
      data_color(
        rows = get_succ_index,
        columns = `Get Endpoint`,
        palette = happy_green_1
      )
  }

  if (length(get_fail_index) > 0) {
    tmp <- tmp |>
      data_color(
        rows = get_fail_index,
        columns = `Get Endpoint`,
        palette = happy_red_1
      )
  }

  if (length(ext_succ_index) > 0) {
    tmp <- tmp |>
      data_color(
        rows = ext_succ_index,
        columns = `Extract Data`,
        palette = happy_green_2
      )
  }

  if (length(ext_fail_index) > 0) {
    tmp <- tmp |>
      data_color(
        rows = ext_fail_index,
        columns = `Extract Data`,
        palette = happy_red_2
      )
  }

  if (length(bld_succ_index) > 0) {
    tmp <- tmp |>
      data_color(
        rows = bld_succ_index,
        columns = `Build Table`,
        palette = happy_green_3
      )
  }

  if (length(bld_fail_index) > 0) {
    tmp <- tmp |>
      data_color(
        rows = bld_fail_index,
        columns = `Build Table`,
        palette = happy_red_3
      )
  }
  tmp
}

getCompletionSummary <- function(DT) {
  ctabs <- sort(names(DT))

  argsDT <- rbindlist(rdleaflogix:::get_pipeline_args("leaflogix"))
  orgs_processed <- argsDT[, .N, keyby = .(org)][, !"N"]
  orgIndex <- read_org_index()

  sapply(ctabs, function(tb) {
    # tb <- ctabs[1]
    dt <- DT[[tb]]
    if (nrow(dt) > 0) {
      locSumm <- dt[, .(total_rows = .N), keyby = .(org, store)]
      filtIndex <- setkey(orgIndex, org)[locSumm[, .N, keyby = .(org)][, .(org)]]
      tmp <- setcolorder(setkey(filtIndex, org, store)[locSumm], c("org_uuid", "store_uuid"))
      tmp[is.na(total_rows), total_rows := 0]

      # tmp[orgs_processed, .(org_uuid, org)][, .N, .(org_uuid, org)][, !"N"]
      OUT <- tmp[orgs_processed, .(
        pct_stores_complete = sum(total_rows > 0) / .N,
        total_rows = sum(total_rows)
      ), by = .EACHI]

      OUT[is.na(pct_stores_complete), pct_stores_complete := 0]
      OUT[is.na(total_rows), total_rows := 0]

      OUT[, pct_stores_complete := scales::percent(pct_stores_complete, accuracy = 1)]
      OUT[, total_rows := scales::comma(total_rows)]


      OUT <- OUT[orgIndex[, .N, keyby = .(org, org_uuid)], nomatch = NULL][, !"N"]

      setcolorder(OUT, "org_uuid")


      colr_rows <- OUT[stringr::str_detect(pct_stores_complete, "100%", negate = TRUE), which = TRUE]


      # Note no filtering is done in this pipeline. These rows are 100% of the data received from the API.
      #
      DT <- OUT |>
        gt() |>
        cols_label(
          org_uuid = md("**org_uuid**"),
          org = md("**org**"),
          pct_stores_complete = md("**pct_stores_complete**"),
          total_rows = md("**total_rows**")
        ) |>
        tab_header(
          title = paste0("consolidated.leaflogix.", tb),
          subtitle = "Percentage of Stores Completed"
        )



      if (length(colr_rows) > 0) {
        DT  |>
          data_color(
            rows = colr_rows,
            palette = happy_gold_2
          )
      } else {
        DT
      }
    }

  }, simplify = FALSE, USE.NAMES = TRUE)

}




