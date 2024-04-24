box::use(
  dplyr[mutate]
)

anonymize_demoorg <- function(inventory) {
  mutate(
    inventory,
    name = gsub("mmd", "House Brand", name, ignore.case = TRUE),
    brand = gsub("mmd", "House Brand", brand, ignore.case = TRUE),
  )
}
