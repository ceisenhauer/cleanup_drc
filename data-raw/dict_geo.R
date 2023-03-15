## code to prepare `dict_geo` dataset goes here
dict_geo <- rio::import(here::here('data-raw', 'dict_geo.csv'))

usethis::use_data(dict_geo,
                  internal = TRUE,
                  overwrite = TRUE)
