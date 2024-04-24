# Polaris API
# run.R
#
# (C) Happy Cabbage Analytics, Inc. 2021

box::use(
  polarispub[run_email_period_campaigns]
)

# RUN OBSERVER
succeeded_runs <- run_email_period_campaigns(period_mins = 30)

if (any(!succeeded_runs)) {
  stop(
    "Failed to send ", sum(!succeeded_runs), " campaigns; succeded to send ", sum(succeeded_runs)
  )
}
