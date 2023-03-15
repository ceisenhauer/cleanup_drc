#' Clean and Save IDS Surveillance Data
#'
#' @description Loads and cleans IDS data with the option to save the cleaned output to a file in 
#'   addition to loading it into the environment.
#'
#' @param data_file `str` File to be loaded and cleaned.
#' @param data_root `str` Directory where data is stored. Default is 'data'.
#' @param year `int` Year to associate with the data in `data_file`. Default is the current year.
#' @param save `bool` If `TRUE`, the output data will be saved to a file. Default is `TRUE`.
#' @param save_root `str` Path to be used when saving the output data (if `save` is `TRUE`). Default
#'   is 'current_clean.csv'.
#'
#' @return df
#'
#' @import tidytable
#' @importFrom dplyr recode
#' @importFrom tinker iso_to_date %notin%
#' @importFrom here here
#' @importFrom Hmisc mdb.get
#' @importFrom linelist clean_data
#' @importFrom rio ipmort export
#' @importFrom tidyr complete
clean_ids <- function(data_file = 'ids.xlsx', data_root = here::here('data'), maladie = NULL,
                      year = as.numeric(format(Sys.Date(), '%Y')), max_week = NULL,
                      save = TRUE, save_root = here::here('out')) {

# LOAD ------------------------------------------------------------------------------------------
  fn <- here::here(data_root, data_file)
  ext <- tools::file_ext(data_file)

  # extension specific import, throw warning if type is unrecognized
  if (ext == 'MDB') {
    df <- Hmisc::mdb.get(fn) 
  } else if (ext == 'xlsx') {
    df <- rio::import(fn)
  } else {
    warning(pate0('Unrecognized file type for IDS data. Expected MDB or xlsx but found ', ext,
                  '.\nAttempting to import with rio...'))
    df <- rio::import(fn)
  }



  # WRANGLE ----------------------------------------------------------------------------------------

  # SELECT (AND LOWERCASE ASCII ONLY PLEASE) -----
  df <- df %>%
          linelist::clean_data() %>%
          tidytable::mutate.(tidytable::across.(tidytable::contains(c('mois', 'ans')), as.numeric))
        

  # NB: starting in 2021 data splits 5-15 years old and 15+ into seperate categories but our
  # analysis will still keep them as a single 5+ year old group 
  if (year > 2020) {
    df <- df %>%
            tidytable::select.(prov, zs, pop, maladie, numsem, c011mois, d011mois, c1259mois,
                               d1259mois, c515ans, d515ans, cp15ans, dp15ans)
  } else {
    df <- df %>%
            tidytable::select.(prov, zs, pop, maladie, numsem, c011mois, d011mois, c1259mois, 
                               d1259mois, c5ansp, d5ansp) 
  }

  # ADD INDICATORS -----
  # if new data, combine 5-15 and 15+ age brackets then calc total cases
  if (year > 2020) {
    df <- df %>%
            tidytable::mutate_rowwise.(c5ansp = tinker::.sum(c515ans, cp15ans),
                                       d5ansp = tinker::.sum(d515ans, dp15ans)) %>% 
            tidytable::select.(!tidytable::ends_with('ans'))
  }

  df <- df %>%
          tidytable::mutate_rowwise.(totalcas = tinker::.sum(c011mois, c1259mois, c5ansp),
                                     totaldeces = tinker::.sum(d011mois, d1259mois, d5ansp)) 
                                

  # CLEAN -----
  # remove observations with dates in the future
  # TODO: log file with any entries that were removed, this would be great but is low priority as
  # we haven't seen any futuredate issues 
  if (year == lubridate::year(Sys.Date())) {
    df <- df %>%
            tidytable::filter.(numsem <= lubridate::week(Sys.Date()))
  } else {
    df <- df %>%
            tidytable::filter.(numsem <= 53)
  }


  # name spelling issues
  fixes <- list('bafwabgobgo' = 'bafwagbogbo',
                'mambassa' = 'mambasa',
                'ruzizi03' = 'ruzizi',
                'shabunda_centre' = 'shabunda',
                'gbado_lite' = 'gbadolite',
                'mobayi_mbongo15' = 'mobayi_mbongo',
                'mobayi_mbogo' = 'mobayi_mbongo',
                'mobatyi_mbongo' = 'mobayi_mbongo',
                'mobayi_ubangi' = 'mobayi_mbongo',
                'waosolo' = 'wasolo',
                'was0lo' = 'wasolo',
                'yalifafo' = 'yalifafu',
                'gadolite' = 'gbadolite',
                'gbadlite' = 'gbadolite',
                'gbadplite' = 'gbadolite',
                'gbadplite' = 'gbadolite',
                'wapida' = 'wapinda',
                'bena_tshadi' = 'bena_tshiadi',
                'bosbolo' = 'bosobolo',
                'djalo_djeka' = 'djalo_ndjeka',
                'omendjadi' = 'omondjadi',
                'kalonda' = 'kalonda_ouest',
                'benetshiadi' = 'bena tshiadi',
                'bobobzo' = 'bobozo',
                'bukonde' = 'bunkonde',
                'bunkone' = 'bunkonde',
                'lubobdaie' = 'lubondaie',
                'muetshi' = 'mwetshi',
                'shikula' = 'tshikula',
                'yanfgala' = 'yangala',
                'omendjadi' = 'omondjadi',
                'dibele' = 'bena_dibele',
                'kiambi' = 'kiyambi',
                'mbulula' = 'mbulala',
                'kakene' = 'kakenge',
                'kakengge' = 'kakengge',
                'kamonia' = 'kamonya',
                'kamuesha' = 'kamwesha',
                'kamweshe' = 'kamwesha',
                'kekenge' = 'kakenge',
                'kitangua' = 'kitangwa',
                'mueka' = 'mweka'
  )

  df <- df %>%
          tidytable::mutate.(zs = dplyr::recode(zs, !!!fixes),
                             prov_zs = paste0(prov, '#', zs))


  # MAKE ZONES AND DATES WITH NO DATA EXPLICIT -----
  # expand dates and add date for start of week
  max_week <- ifelse(is.null(max_week), tinker::.max(df$numsem), max_week)
  writeLines(paste0('max week is : ', max_week))

  df_all <- data.frame()

  for (mal in unique(df$maladie)) {
    writeLines(paste0('processing ', mal, '...'))

    tmp <- df %>%
            tidytable::filter.(maladie == mal)

    missing_zones <- geo_dictionary %>%
                       tidytable::filter.(prov_zs %notin% unique(tmp$prov_zs)) %>%
                       tidytable::mutate.(maladie = mal,
                                          numsem = 1)

    tmp <- tmp %>%
            tidytable::bind_rows.(missing_zones) %>%
            tidytable::complete.(numsem = seq(1, max_week),
                                 .by = c(prov, zs, prov_zs)) %>%
            tidytable::mutate.(maladie = mal)

    tmp <- tmp %>%
            tidytable::mutate.(year = year,
                               debutsem = tinker::iso_to_date(week = numsem,
                                                              year = year))

    # REMOVE DUPLICATES -----
    # when rows have duplicated id columns (province, zone, date) but conflicting case / death data,
    # choose the row with the highest number of total cases, then total deaths, then cases over 5, etc
    # TODO: save duplicate entries to a log file so that they can be reported to the national
    # surveillance system for correction -- consider splitting these into full duplicates and rows 
    # that duplicate the id columns (province, health zone, date) but have different case / death data
    tmp <- tmp %>%
            tidytable::arrange.(totalcas, totaldeces, c5ansp, d5ansp, c1259mois, d1259mois, c011mois,
                                d011mois) %>%
            tidytable::distinct.(prov, zs, debutsem, .keep_all = TRUE) %>%
            tidytable::arrange.(prov, zs, debutsem) %>%
            tidytable::select.(prov, zs, prov_zs, maladie, numsem, year, debutsem, c011mois, 
                   c1259mois, c5ansp, totalcas, d011mois, d1259mois, d5ansp, totaldeces) %>%
            tidytable::rename.(reg = prov,
                               zone = zs,
                               reg_zone = prov_zs,
                               week = numsem,
                               date = debutsem,
                               cases_0_11 = c011mois,
                               cases_12_59 = c1259mois,
                               cases_5plus = c5ansp,
                               cases = totalcas,
                               deaths_0_11 = d011mois,
                               deaths_12_59 = d1259mois,
                               deaths_5plus = d5ansp,
                               deaths = totaldeces)

    # SAVE -------------------------------------------------------------------------------------------
    if (save) {
      rio::export(tmp, paste0('clean_', mal, '.csv'))
    }

    df_all <- bind_rows(df_all, tmp)
  }

  if (save) {
    rio::export(df_all, paste0('clean_ids.csv'))
  }
  
  # RETURN -----------------------------------------------------------------------------------------
  return(df_all)
}
