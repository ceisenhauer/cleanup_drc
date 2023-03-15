# create zone dictionary
geo_dictionary <- readRDS(here::here('raw-data/pops.RDS')) %>%
  select(prov, zs, prov_zs, pop)

usethis::use_data(geo_dictionary)
