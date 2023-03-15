## code to prepare `dict_maladies` dataset goes here
maladies <- rio::import(here::here('data-raw', 'dict_maladies.csv'))

dict_maladie <- maladies$original
names(dict_maladie) <- maladies$maladie

usethis::use_data(dict_maladie,
                  overwrite = TRUE,
                  internal = TRUE)
