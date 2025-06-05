

HomeStation <- R6::R6Class("HomeStation",
                       list(connection = NULL,
                            cdmSchema = NULL,
                            cohortTableName = NULL,
                            initialize = function(connection,
                                                  cdmSchema,
                                                  cohortTableName){
                              stopifnot(class(con) == "DatabaseConnectorJdbcConnection")

                              self$connection <- connection
                              self$cdmSchema <- cdmSchema
                              self$cohortTableName <- cohortTableName
                            }))

HomeStation$set("public", "getHomeStation",
                function(){

                  cohort <- dplyr::tbl(self$connection,
                                       I(self$cohortTableName))


                  conditionOccurrence <- dplyr::tbl(self$connection,
                                                     I(paste0(self$cdmSchema,
                                                              ".condition_occurrence"))) |>
                    dplyr::select(person_id,
                                  condition_start_date,
                                  visit_detail_id) |>
                    dplyr::rename(subject_id = person_id)


                  visitDetail <- dplyr::tbl(self$connection,
                                            I(paste0(self$cdmSchema,
                                                     ".visit_detail"))) |>
                    dplyr::select(visit_detail_id,
                                  care_site_id)

                  careSite <- dplyr::tbl(self$connection,
                                         I(paste0(self$cdmSchema,
                                                  ".care_site"))) |>
                    dplyr::select(care_site_id, x_institutioncode)


                  dplyr::left_join(cohort,
                                   conditionOccurrence) |>
                    dplyr::filter(between(condition_start_date,
                                          cohort_start_date,
                                          cohort_end_date)) |>
                    dplyr::distinct(cohort_definition_id,
                             subject_id,
                             condition_start_date,
                             .keep_all = TRUE) |>
                    dplyr::left_join(visitDetail) |>
                    dplyr::filter(!is.na(care_site_id)) |>
                    dplyr::left_join(careSite) |>
                    dplyr::select(-visit_detail_id, -care_site_id) |>
                    dplyr::rename(station = x_institutioncode) |>
                    dplyr::collect() -> self$homeStation

                  invisible(self)

                })
