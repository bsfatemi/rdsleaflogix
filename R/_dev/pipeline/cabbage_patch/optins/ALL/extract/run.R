library(magrittr)

# SOURCE
source("inst/pipeline/cabbage_patch/optins/ALL/extract/src/extract_form_optins.R")

# READ
raw <- pipelinetools::rd_raw_archive("optins", "ALL", "hca")

# RUN
out <- extract_form_optins(raw)

if (!is.null(out)) {
  pg <- hcaconfig::dbc("prod2", "cabbage_patch")
  DBI::dbWriteTable(pg, "form_optins2", value = out, append = TRUE)
  DBI::dbDisconnect(pg)
} else {
  print(stringr::str_glue("{Sys.time()} NO VALID OPT-INS"))
}
