#' Clean and Save IDS Surveillance Data
#'
#' @description Loads and cleans IDS data with the option to save the cleaned output to a file in 
#'   addition to loading it into the environment.
#'
#' @param data_file `str` File to be loaded and cleaned.
#' @param data_root `str` Directory where data is stored. Default is 'data'.
#' @param year `int` Year to associate with the data in `data_file`. Default is the current year.
#' @param save `bool` If `TRUE`, the output data will be saved to a file. Default is `TRUE`.
#' @param save_path `str` Path to be used when saving the output data (if `save` is `TRUE`). Default
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
                      save = TRUE, 
                      save_path = 'ids.csv') {

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

  if (!is.null(maladie)) {
    maladie <- toupper(maladie)

    if (maladie %in% unique(df$MALADIE)) {
      df <- df %>%
              tidytable::filter.(MALADIE == 'CHOLERA')
    } else {
      warning(paste0('maladie \'', maladie, '\' not found. returning data for all causes. for',
                     'reference, available options include : ',
                     paste(unique(df$MALADIE), sep = ', ')))
    }
  }

  # SELECT (AND LOWERCASE ASCII ONLY PLEASE) -----
  df <- df %>%
          linelist::clean_data() %>%
          tidytable::mutate.(tidytable::across.(dplyr::contains(c('mois', 'ans')), as.numeric))
        

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
                                       d5ansp = tinker::.sum(d515ans, cp15ans)) %>% 
            tidytable::select.(!ends_with('ans'))
  }

  df <- df %>%
          tidytable::mutate_rowwise.(totalcas = tinker::.sum(c011mois, c1259mois, c5ansp),
                                     totaldeces = tinker::.sum(d011mois, d1259mois, d5ansp)) 
                                

  # CLEAN -----
  # remove observations with dates in the future
  # TODO: log file with any entries that were removed, this would be great but is low priority as
  # we haven't seen any futuredate issues 
  df <- df %>%
          tidytable::filter.(numsem <= 53)

  if (year == lubridate::year(Sys.Date())) {
    df <- df %>%
            tidytable::filter.(numsem <= lubridate::week(Sys.Date()))
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
                'mbulula' = 'mbulala')

  df <- df %>%
          tidytable::mutate.(zs = dplyr::recode(zs, !!!fixes),
                             prov_zs = paste0(prov, '#', zs))


  # MAKE ZONES AND DATES WITH NO DATA EXPLICIT -----
  # build and save df of most recent population data available
  pops <- df %>%
            tidytable::summarize.(pop = tidytable::last.(stats::na.omit(pop)),
                                  .by = c(prov_zs, prov, zs))

  pops <- readRDS(here::here('data', 'population_data.RDS')) %>%
           tidytable::filter.(prov_zs %notin% pops$prov_zs) %>%
           tidytable::bind_rows.(pops) %>%
           tidytable::mutate.(numsem = 1)

  #saveRDS(pops, here::here('data', 'population_data.RDS'))

  # expand dates and add date for start of week
  max_week <- ifelse(is.null(max_week), tinker::.max(df$numsem), max_week)
  missing_zones <- pops %>%
                     tidytable::filter.(prov_zs %notin% unique(df$prov_zs))

  df <- df %>%
          tidytable::bind_rows.(missing_zones) %>%
          tidytable::complete.(numsem = seq(1, max_week),
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
          tidytable::mutate.(year = year,
                             debutsem = tinker::iso_to_date(week = numsem,
                                                            year = year),
                             pop = tidytable::last.(stats::na.omit(pop)),
                             .by = c(prov, zs, prov_zs))


  # REMOVE DUPLICATES -----
  # when rows have duplicated id columns (province, zone, date) but conflicting case / death data,
  # choose the row with the highest number of total cases, then total deaths, then cases over 5, etc
  # TODO: save duplicate entries to a log file so that they can be reported to the national
  # surveillance system for correction -- consider splitting these into full duplicates and rows 
  # that duplicate the id columns (province, health zone, date) but have different case / death data
  df <- df %>%
          tidytable::arrange.(totalcas, totaldeces, c5ansp, d5ansp, c1259mois, d1259mois, c011mois,
                              d011mois) %>%
          tidytable::distinct.(prov, zs, debutsem, .keep_all = TRUE) %>%
          tidytable::arrange.(prov, zs, debutsem)

  # VALIDATE -----
	# realistic dates
	# no missing zones / provinces
	# all zones present with correct number of observations
  # pops should be unique (ex. more than a handful of zones w/ same pop is sus)
  # no duplicate entries (wrt prov zone date)
  # no missing data for year, week, date
  


#assert("T is bad for TRUE, and so is F for FALSE", {
    #T = FALSE
    #F = TRUE
    #(T != TRUE)  # note the parentheses
    #(F != FALSE)
#})

  # SAVE -------------------------------------------------------------------------------------------
  if (save) {
    rio::export(df, save_path)
  }

  
  # RETURN -----------------------------------------------------------------------------------------
  return(df)
}
