#' Clean and Save IDS Surveillance Data
#'
#' @description Loads and cleans IDS data with the option to save the cleaned output to a file in 
#'   addition to loading it into the environment.
#'
#' @param data_file `str` File to be loaded and cleaned.
#' @param data_root `str` Directory where data is stored. Default is 'data'.
#' @param maladie `str` Maladie to extract and clean. 
#' @param year `int` Year to associate with the data in `data_file`. Default is the current year.
#' @param add_history `bool` If `TRUE`, historic IDS data (from 2019) will be added included in the
#'   output. Default is `TRUE`.
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
mod_prepare_IDS <- function(data_file = NULL, data_root = here::here('data', 'ids'),
                            maladie = 'measles',
                            year = as.numeric(format(Sys.Date(), '%Y')), max_week = NULL,
                            add_history = TRUE, save = TRUE, 
                            save_root = here::here('out')) {

  # ARGUMENT VALIDATION ----------------------------------------------------------------------------
  if (maladie %notin% dict_malaie$maladie) {
    stop(paste0('Apologies, there is no data for ', maladie, 'in the IDS data. Please select one',
                ' of the following: \n', paste(dict_maladie$maladie, sep = '\n')))
  }

  # TODO: consider if it's better to accept a vector of maladies and then return either a single df
  # or perhaps a named list (or have the option of both)
  if (length(maladie) > 1) {
    stop(paste0('Sorry, currently `clean_ids()` cannot clean multiple maladies in a single call. ',
                'Please implement a for loop instead'))
  }

  maladie_ids <- dict_maladie[[maladie]]


  # LOAD -------------------------------------------------------------------------------------------
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
          filter.(MALADIE == maladie_ids) %>%
          linelist::clean_data() %>%
          mutate.(across.(contains(c('mois', 'ans')), as.numeric))
        

  # NB: starting in 2021 data splits 5-15 years old and 15+ into seperate categories but our
  # analysis will still keep them as a single 5+ year old group 
  if (year > 2020) {
    df <- df %>%
            select.(prov, zs, pop, numsem, c011mois, d011mois, c1259mois, d1259mois,
                    c515ans, d515ans, cp15ans, dp15ans)
  } else {
    df <- df %>%
            select.(prov, zs, pop, numsem, c011mois, d011mois, c1259mois, d1259mois,
                    c5ansp, d5ansp) 
  }

  # ADD INDICATORS -----
  # if new data, combine 5-15 and 15+ age brackets then calc total cases
  if (year > 2020) {
    df <- df %>%
            mutate_rowwise.(c5ansp = tinker::.sum(c515ans, cp15ans),
                            d5ansp = tinker::.sum(d515ans, dp15ans)) %>% 
            select.(!ends_with('ans'))
  }

  df <- df %>%
          mutate_rowwise.(totalcas = tinker::.sum(c011mois, c1259mois, c5ansp),
                          totaldeces = tinker::.sum(d011mois, d1259mois, d5ansp)) 
                                

  # CLEAN -----
  # remove observations with dates in the future
  # TODO: log file with any entries that were removed, this would be great but is low priority as
  # we haven't seen any futuredate issues 
  df <- df %>%
          filter.(numsem <= 53)

  if (year == lubridate::year(Sys.Date())) {
    df <- df %>%
            filter.(numsem <= lubridate::week(Sys.Date()))
  }


  # name spelling issues
  fixes <- list(
                'bafwabgobgo' = 'bafwagbogbo',
                'bena_tshadi' = 'bena_tshiadi',
                'benetshiadi' = 'bena tshiadi',
                'bobobzo' = 'bobozo',
                'bogosenubia' = 'bogosenubea',
                'bosbolo' = 'bosobolo',
                'boso_manzi' = 'bosomanzi',
                'bukonde' = 'bunkonde',
                'bunkone' = 'bunkonde',
                'dibele' = 'bena_dibele',
                'djalo_djeka' = 'djalo_ndjeka',
                'gadolite' = 'gbadolite',
                'gbadlite' = 'gbadolite',
                'gbado_lite' = 'gbadolite',
                'gbadplite' = 'gbadolite',
                'gbadplite' = 'gbadolite',
                'kalonda' = 'kalonda_ouest',
                'kamonia' = 'kamonya',
                'kiambi' = 'kiyambi',
                'kimbi_lulenge' = 'lulenge_kimbi',
                'lilanga_bobanga' = 'lilanga_bobangi',
                'lubobdaie' = 'lubondaie',
                'mambassa' = 'mambasa',
                'mbulula' = 'mbulala',
                'mobatyi_mbongo' = 'mobayi_mbongo',
                'mobayi_mbogo' = 'mobayi_mbongo',
                'mobayi_mbongo15' = 'mobayi_mbongo',
                'mobayi_ubangi' = 'mobayi_mbongo',
                'muetshi' = 'mwetshi',
                'omendjadi' = 'omondjadi',
                'omendjadi' = 'omondjadi',
                'pimo' = 'pimu',
                'ruzizi03' = 'ruzizi',
                'shabunda_centre' = 'shabunda',
                'shikula' = 'tshikula',
                'vangakete' = 'vanga_kete',
                'waosolo' = 'wasolo',
                'wapida' = 'wapinda',
                'was0lo' = 'wasolo',
                'wembonyama' = 'wembo_nyama',
                'yalifafo' = 'yalifafu',
                'yanfgala' = 'yangala'
                )

  df <- df %>%
          mutate.(zs = dplyr::recode(zs, !!!fixes),
                  prov = dplyr::recode(prov,
                                       'mai_ndombe' = 'maindombe'),
                  prov = dplyr::case_when(prov == 'lualaba' & zs == 'kashobwe' ~ 'haut_katanga',
                                          TRUE ~ prov),
                  prov_zs = paste0(prov, '#', zs))


  # MAKE ZONES AND DATES WITH NO DATA EXPLICIT -----
  # build and save df of most recent population data available
  #pops <- df %>%
            #summarize.(pop = last.(na.omit(pop)),
                       #.by = c(prov_zs, prov, zs))

  #pops <- readRDS(here::here('data', 'reference', 'population_data.RDS')) %>%
            #filter.(prov_zs %notin% pops$prov_zs) %>%
            #bind_rows.(pops) %>%
            #mutate.(numsem = 1)

  #saveRDS(pops, here::here('data', 'reference', 'population_data.RDS'))

  # expand dates and add date for start of week
  max_week <- ifelse(is.null(max_week), max(df$numsem, na.rm = TRUE), max_week)

  ref_zones <- rio::import(here::here('data', 'reference', 'geo_dictionary.csv'))
  missing_zones <- ref_zones %>%
                     mutate.(numsem = 1) %>%
                     filter.(prov_zs %notin% unique(df$prov_zs))

  extra_zones <- df %>%
    filter.(prov_zs %notin% unique(ref_zones$prov_zs)) %>%
    select.(prov, zs) %>%
    distinct.()

  if (nrow(extra_zones > 0)) {
    rlang::abort('oh no, unknown zones detected ! stopping early...',
          x = extra_zones)
    return(x)
  }

  df <- df %>%
          bind_rows.(missing_zones) %>%
          complete.(numsem = seq(1, max_week),
                    .by = c(prov, zs, prov_zs))

  # if year is already in df, keep it
  # this requires work on max week stuff
  #if ('annee' %in% names(df)) {
    #df <- df %>%
            #rename(annee = 'year')
  #} else {
    #df <- df %>%
            #rename(year = year)
  #}

  df <- df %>%
          mutate.(#year = year,
                  debutsem = tinker::iso_to_date(week = numsem,
                                                 year = year),
                  .by = c(prov, zs, prov_zs))



  # REMOVE DUPLICATES -----
  # when rows have duplicated id columns (province, zone, date) but conflicting case / death data,
  # choose the row with the highest number of total cases, then total deaths, then cases over 5, etc
  # TODO: save duplicate entries to a log file so that they can be reported to the national
  # surveillance system for correction -- consider splitting these into full duplicates and rows 
  # that duplicate the id columns (province, health zone, date) but have different case / death data
  # TODO: TODO: ACTUALLY just stop being lazy and implement a report indicating the number of 
  # observations that were altered by : column spec and overall
  df <- df %>%
  arrange.(totalcas, totaldeces, c5ansp, d5ansp, c1259mois, d1259mois, c011mois,
           d011mois) %>%
  distinct.(prov, zs, debutsem, .keep_all = TRUE) %>%
  arrange.(prov, zs, debutsem) %>%
  rename.(reg = prov,
          zone = zs,
          reg_zone = prov_zs,
          week = numsem,
          date = debutsem,
          cases_0_11 = c011mois,
          cases_12_59 = c1259mois,
          cases_over_5 = c5ansp,
          cases = totalcas,
          deaths_0_11 = d011mois,
          deaths_12_59 = d1259mois,
          deaths_over_5 = d5ansp,
          deaths = totaldeces) %>%
  mutate.(year = lubridate::isoyear(date)) %>%
  select.(reg, zone, reg_zone, year, week, date, starts_with('cases'), starts_with('deaths'))


  # ADD HISTORY ------------------------------------------------------------------------------------
  if (add_history) {
    df <- readRDS(here::here('data', 'static_data', 'IDS_', maladie, '_history.RDS')) %>%
            bind_rows(df) %>%
            arrange.(reg_zone, year, week)
  }

  # SAVE -------------------------------------------------------------------------------------------
  if (save) {
    rio::export(df,
                paste0(here::here(output_root, 'ids_', maladie, '.RDS')))
  }

  
  # RETURN -----------------------------------------------------------------------------------------
  return(df)
}
