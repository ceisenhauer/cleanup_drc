#' Clean and Save IDS Surveillance Data
#'
#' @description Loads and cleans IDS data with the option to save the cleaned output to a file in 
#'   addition to loading it into the environment.
#'
#' @param data_file `str` File to be loaded and cleaned.
#' @param data_root `str` Directory where data is stored. Default is 'data'.
#' @param maladie `str` Maladie to extract and clean. 
#' @param year `int` Year to associate with the data in `data_file`. Default is the current year.
#' @param max_week `int` The maximum week expected in the dataset.
#' @param add_history `bool` If `TRUE`, historic IDS data (from 2019) will be added included in the
#'   output. Default is `TRUE`.
#' @param save `bool` If `TRUE`, the output data will be saved to a file. Default is `TRUE`.
#' @param output_root `str` Path to be used when saving the output data (if `save` is `TRUE`). Default
#'   is 'current_clean.csv'.
#'
#' @return df
#'
#' @importFrom tidytable filter. mutate. across. select. mutate_rowwise. distinct. bind_rows.
#'   complete. arrange. rename. 
#' @importFrom dplyr %>% recode contains starts_with ends_with
#' @importFrom tinker iso_to_date %notin%
#' @importFrom here here
#' @importFrom Hmisc mdb.get
#' @importFrom linelist clean_data
#' @importFrom lubridate isoweek isoyear
#' @importFrom rio import export
#' @importFrom rlang abort .data
#' @importFrom tidyr complete
#' @export
clean_ids <- function(data_file = NULL, data_root = here::here('data', 'ids'),
                      maladie = 'measles',
                      year = as.numeric(format(Sys.Date(), '%Y')), max_week = NULL,
                      add_history = TRUE, save = TRUE, 
                      output_root = here::here('out')) {

  # ARGUMENT VALIDATION ----------------------------------------------------------------------------
  if (maladie %notin% names(dict_maladie)) {
    stop(paste0('Apologies, there is no data for ', maladie, 'in the IDS data. Please select one',
                ' of the following: \n', paste(dict_maladie[[maladie]], sep = '\n')))
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
    warning(paste0('Unrecognized file type for IDS data. Expected MDB or xlsx but found ', ext,
                  '.\nAttempting to import with rio...'))
    df <- rio::import(fn)
  }



  # WRANGLE ----------------------------------------------------------------------------------------

  # SELECT (AND LOWERCASE ASCII ONLY PLEASE) -----
  df <- df %>%
          filter.(.data$MALADIE == maladie_ids) %>%
          linelist::clean_data() %>%
          mutate.(across.(contains(c('mois', 'ans')), as.numeric))
        

  # NB: starting in 2021 data splits 5-15 years old and 15+ into seperate categories but our
  # analysis will still keep them as a single 5+ year old group 
  if (year > 2020) {
    df <- df %>%
            select.(.data$prov, .data$zs, .data$pop, .data$numsem, .data$c011mois, .data$d011mois,
                    .data$c1259mois, .data$d1259mois, .data$c515ans, .data$d515ans, .data$cp15ans,
                    .data$dp15ans)
  } else {
    df <- df %>%
            select.(.data$prov, .data$zs, .data$pop, .data$numsem, .data$c011mois, .data$d011mois,
                    .data$c1259mois, .data$d1259mois, .data$c5ansp, .data$d5ansp) 
  }

  # ADD INDICATORS -----
  # if new data, combine 5-15 and 15+ age brackets then calc total cases
  if (year > 2020) {
    df <- df %>%
            mutate_rowwise.(c5ansp = tinker::.sum(.data$c515ans, .data$cp15ans),
                            d5ansp = tinker::.sum(.data$d515ans, .data$dp15ans)) %>% 
            select.(!ends_with('ans'))
  }

  df <- df %>%
          mutate_rowwise.(totalcas = tinker::.sum(.data$c011mois, .data$c1259mois, .data$c5ansp),
                          totaldeces = tinker::.sum(.data$d011mois, .data$d1259mois, .data$d5ansp)) 
                                

  # CLEAN -----
  # remove observations with dates in the future
  # TODO: log file with any entries that were removed, this would be great but is low priority as
  # we haven't seen any futuredate issues 
  df <- df %>%
          filter.(.data$numsem <= 53)

  if (year == lubridate::isoyear(Sys.Date())) {
    df <- df %>%
            filter.(.data$numsem <= lubridate::isoweek(Sys.Date()))
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
          mutate.(zs = dplyr::recode(.data$zs, !!!fixes),
                  prov = dplyr::recode(.data$prov,
                                       'mai_ndombe' = 'maindombe'),
                  prov = dplyr::case_when(.data$prov == 'lualaba' &
                                            .data$zs == 'kashobwe' ~ 'haut_katanga',
                                          TRUE ~ .data$prov),
                  prov_zs = paste0(.data$prov, '#', .data$zs))


  # MAKE ZONES AND DATES WITH NO DATA EXPLICIT -----
  # expand dates and add date for start of week
  max_week <- ifelse(is.null(max_week), max(df$numsem, na.rm = TRUE), max_week)

  ref_zones <- rio::import(here::here('data', 'reference', 'dict_geo.csv'))
  missing_zones <- ref_zones %>%
                     mutate.(numsem = 1) %>%
                     filter.(.data$prov_zs %notin% unique(df$prov_zs))

  extra_zones <- df %>%
    filter.(.data$prov_zs %notin% unique(ref_zones$prov_zs)) %>%
    select.(.data$prov, .data$zs) %>%
    distinct.()

  if (nrow(extra_zones > 0)) {
    rlang::abort('oh no, unknown zones detected ! stopping early...',
                 x = extra_zones)
    return(x)
  }

  df <- df %>%
          bind_rows.(missing_zones) %>%
          complete.(numsem = seq(1, max_week),
                    .by = c(.data$prov, .data$zs, .data$prov_zs))

  df <- df %>%
          mutate.(#year = year,
                  debutsem = tinker::iso_to_date(week = .data$numsem,
                                                 year = .data$year),
                  .by = c(.data$prov, .data$zs, .data$prov_zs))



  # REMOVE DUPLICATES -----
  # when rows have duplicated id columns (province, zone, date) but conflicting case / death data,
  # choose the row with the highest number of total cases, then total deaths, then cases over 5, etc
  # TODO: save duplicate entries to a log file so that they can be reported to the national
  # surveillance system for correction -- consider splitting these into full duplicates and rows 
  # that duplicate the id columns (province, health zone, date) but have different case / death data
  # TODO: TODO: ACTUALLY just stop being lazy and implement a report indicating the number of 
  # observations that were altered by : column spec and overall
  df <- df %>%
  arrange.(.data$totalcas, .data$totaldeces, .data$c5ansp, .data$d5ansp, .data$c1259mois, 
           .data$d1259mois, .data$c011mois, .data$d011mois) %>%
  distinct.(.data$prov, .data$zs, .data$debutsem, .keep_all = TRUE) %>%
  arrange.(.data$prov, .data$zs, .data$debutsem) %>%
  rename.(reg = .data$prov,
          zone = .data$zs,
          reg_zone = .data$prov_zs,
          week = .data$numsem,
          date = .data$debutsem,
          cases_0_11 = .data$c011mois,
          cases_12_59 = .data$c1259mois,
          cases_over_5 = .data$c5ansp,
          cases = .data$totalcas,
          deaths_0_11 = .data$d011mois,
          deaths_12_59 = .data$d1259mois,
          deaths_over_5 = .data$d5ansp,
          deaths = .data$totaldeces) %>%
  mutate.(year = lubridate::isoyear(.data$date)) %>%
  select.(.data$reg, .data$zone, .data$reg_zone, .data$year, .data$week, .data$date,
          starts_with('cases'), starts_with('deaths'))


  # ADD HISTORY ------------------------------------------------------------------------------------
  if (add_history) {
    df <- readRDS(here::here('data', 'static_data', 'IDS_', maladie, '_history.RDS')) %>%
            bind_rows.(df) %>%
            arrange.(.data$reg_zone, .data$year, .data$week)
  }

  # SAVE -------------------------------------------------------------------------------------------
  if (save) {
    rio::export(df,
                paste0(here::here(output_root, 'ids_', maladie, '.RDS')))
  }

  
  # RETURN -----------------------------------------------------------------------------------------
  return(df)
}
