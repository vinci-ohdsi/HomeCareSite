

HomeStation <- R6::R6Class("HomeStation",
                       list(connection = NULL,
                            cdmSchema = NULL,
                            cohortTableName = NULL,
                            homeStation = NULL,
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


                  visitOccurrence <- dplyr::tbl(self$connection,
                                            I(paste0(self$cdmSchema,
                                                     ".visit_occurrence"))) |>
                    dplyr::select(person_id,
                                  visit_occurrence_id,
                                  visit_start_date,
                                  care_site_id) |>
                    dplyr::rename(subject_id = person_id) |>
                    filter(care_site_id != 0)


                  careSite <- dplyr::tbl(self$connection,
                                         I(paste0(self$cdmSchema,
                                                  ".care_site"))) |>
                    dplyr::select(care_site_id, x_institutioncode)


                  dplyr::left_join(cohort,
                                   visitOccurrence) |>
                    dplyr::filter(between(visit_start_date,
                                          sql("DATEADD(year, -1, cohort_start_date)"),
                                          cohort_start_date)) |>
                    dplyr::distinct(cohort_definition_id,
                             subject_id,
                             visit_start_date,
                             .keep_all = TRUE) |>
                    dplyr::left_join(careSite) |>
                    dplyr::rename(station = x_institutioncode) |>
                    dplyr::arrange(cohort_definition_id,
                                   subject_id,
                                   cohort_start_date,
                                   desc(visit_start_date)) |>
                    dplyr::distinct(cohort_definition_id,
                                    subject_id,
                                    cohort_start_date,
                                    .keep_all = TRUE) |>
                    dplyr::select(-visit_occurrence_id,
                                  -visit_start_date,
                                  -care_site_id) |>
                    dplyr::collect() -> self$homeStation

                  invisible(self)

                })


HomeStation$set("public", "plot",
                function(){


                })

