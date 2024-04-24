library(magrittr)
source("inst/pipeline/appdata/sol/internal_logs/src/extract_internal_logs.R")

pth <- "/mnt/data/pipeline/raw/internal_logs/hca/"
df <- extract_internal_logs(pth)

pg <- hcaconfig::dbc("prod2", "appdata")
DBI::dbExecute(pg, paste("TRUNCATE TABLE", DBI::SQL("sol.internal_logs")))
DBI::dbWriteTable(pg, DBI::SQL("sol.internal_logs"), value = df, append = TRUE)
DBI::dbDisconnect(pg)
