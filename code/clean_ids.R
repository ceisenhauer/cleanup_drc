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

clean_ids(data_file = 'IDS_2024_SE12.xlsx',
          data_root = here::here('data', 'raw'),
          language = 'fr',
          output_type = c('.rds', '.xlsx'),
          output_root = here::here('data'))


clean_ids <- function(data_file = NULL, data_root = here::here('data', 'ids'),
                      language = c('en', 'fr'),
                      data_year = as.numeric(format(Sys.Date(), '%Y')), max_week = NULL,
                      add_history = TRUE, 
                      save = TRUE, output_type = '.rds',
                      output_root = here::here('data', 'clean')) {

  language <- match.arg(language)

  ref_adm <- rio::import(here::here('data', 'reference', 'adm_reference_COD.rds')) |>
    dplyr::filter(level == 2) |>
    dplyr::select(adm1 = adm1_name,
           adm2 = adm2_name,
           pcode)

  maladies <- rio::import(here::here('data', 'reference',
                                     paste0('dictionary_malady_', language, '.rds'))) |>
    dplyr::pull(malady_name, name = original)

  malady_codes <- rio::import(here::here('data', 'reference',
                                         paste0('dictionary_malady_', language, '.rds'))) |>
    dplyr::pull(malady_codes, name = malady_name)
    
  data_week <- sub('^([^_]+_){2}(SE)([^\\.]+).*', '\\3', data_file) |> as.numeric()

  # LOAD -------------------------------------------------------------------------------------------


  fn <- here::here(data_root, data_file)
  ext <- tools::file_ext(data_file)

  # extension specific import, throw warning if type is unrecognized
  if (ext == 'MDB') {
    df <- Hmisc::mdb.get(fn) 

  } else if (ext == 'xlsx') {
    df <- rio::import(fn)

  } else {
    stop('Unrecognized file type for IDS data. Expected MDB or xlsx but found ', ext)
  }



  # WRANGLE ----------------------------------------------------------------------------------------

  # SELECT (AND LOWERCASE ASCII ONLY PLEASE) -----
  df <- df |>
    janitor::clean_names() |>
    dplyr::select(adm1 = prov,
                  adm2 = zs,
                  pop,
                  week = numsem,
                  malady = maladie,
                  cases_under_one = c011mois,
                  cases_1to5 = c1259mois,
                  cases_5to15 = c515ans,
                  cases_15_plus = cp15ans,
                  deaths_under_one = d011mois,
                  deaths_1to5 = d1259mois,
                  deaths_5to15 = d515ans,
                  deaths_15_plus = dp15ans) |>
    dplyr::mutate(
      malady = dplyr::recode(malady, !!!maladies),
      dplyr::across(c(pop, week, dplyr::contains(c('cases', 'deaths'))), as.numeric),
      cases = rowSums(dplyr::pick(dplyr::contains('cases'))), 
      deaths = rowSums(dplyr::pick(dplyr::contains('deaths'))),
      cases_5_plus = rowSums(dplyr::pick(c(cases_5to15, cases_15_plus))),
      deaths_5_plus = rowSums(dplyr::pick(c(deaths_5to15, deaths_15_plus)))
    ) |>
    dplyr::filter(week <= data_week) |>
    epiclean::clean_adm_names(ref = ref_adm,
                              fn_fixes = here::here('data', 'reference', 'fixes_adm2.csv'))



  # complete with NAs for zones that didn't notify
  missing_zones <- ref_adm |>
    #dplyr::mutate(week = 1) |>
    dplyr::filter(!(pcode %in% unique(df$pcode)))

  ref_adm <- df |>
    dplyr::arrange(dplyr::desc(week)) |>
    dplyr::distinct(pcode, .keep_all = TRUE) |>
    dplyr::select(pcode, pop) |>
    dplyr::right_join(ref_adm) 

  df <- df |>
    dplyr::bind_rows(missing_zones) |>
    tidyr::complete(week = seq(1, data_week),
                    malady = unname(maladies),
                    pcode = ref_adm$pcode) |>
    dplyr::filter(!is.na(malady) & !is.na(week)) |>
    dplyr::arrange(pcode, malady, week, desc(cases)) |>
    dplyr::distinct(pcode, malady, week, .keep_all = TRUE) |>   # cosider saving dups in log
    dplyr::select(-pop, -adm1, -adm2) |>
    dplyr::left_join(ref_adm, by = 'pcode') 



  # add year and date (monday); rearrange to be pretty
  df <- df |>
    dplyr::mutate(.by = c(pcode),
           year = data_year,
           date = tinker::iso_to_date(week = week,
                                      year = year)) |>
    dplyr::select(malady, pcode, adm1, adm2, pop, year, week, date,
                  dplyr::starts_with('cases'), dplyr::starts_with('deaths'))


  # SAVE -------------------------------------------------------------------------------------------
  if (save) {

  # overall ids
  for (ext in output_type) {
    df |>
      rio::export(here::here(output_root, paste0('all_maladies_ids_', language, ext)))
  }

  # malady wise
  for (m in names(malady_codes)) {
    df |>
      dplyr::filter(malady == m) |>
      dplyr::select(-malady) |>
      rio::export(here::here('data', 'clean', paste0(malady_codes[[m]], '_ids.rds')))
  }


  ## ADD HISTORY ------------------------------------------------------------------------------------
  #if (add_history) {
    #df <- readRDS(here::here('data', 'static_data', 'IDS_', maladie, '_history.RDS')) %>%
            #bind_rows.(df) %>%
            #arrange.(.data$reg_zone, .data$year, .data$week)
  #}

  ## SAVE -------------------------------------------------------------------------------------------
  #if (save) {
    #rio::export(df,
                #paste0(here::here(output_root, 'ids_', maladie, '.RDS')))
  #}

  
  # RETURN -----------------------------------------------------------------------------------------
  return(df)
}
