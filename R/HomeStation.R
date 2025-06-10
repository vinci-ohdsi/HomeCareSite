# todo: person table (non-longitudinal) care site as tie-breaker

HomeStation <- R6::R6Class("HomeStation",
                       list(connection = NULL,
                            cdmSchema = NULL,
                            cohortTableName = NULL,
                            visitMethod = NULL,
                            homeStation = NULL,
                            preDays = NULL,
                            cohort = NULL,
                            visitOccurrence = NULL,
                            careSite = NULL,

                            initialize = function(connection,
                                                  cdmSchema,
                                                  cohortTableName,
                                                  visitMethod = "mostRecent",
                                                  preDays = 365,
                                                  primaryCriteriaType = ){
                              stopifnot(class(connection) == "DatabaseConnectorJdbcConnection")
                              stopifnot(visitMethod %in% c("mostRecent", "majority"))
                              stopifnot(primaryCriteriaType %in% c("condition", "drug", "person"))

                              self$connection <- connection
                              self$cdmSchema <- cdmSchema
                              self$cohortTableName <- cohortTableName
                              self$visitMethod <- visitMethod
                              self$preDays <- preDays

                            })
)


HomeStation$set("private", "rankingFunction",
                function(){
                  if (self$visitMethod == "mostRecent") {
                    return(\(x) dplyr::arrange(x,
                                               cohort_definition_id,
                                               subject_id,
                                               cohort_start_date,
                                               desc(visit_start_date)) |>
                      dplyr::select(-visit_occurrence_id,
                             -visit_start_date,
                             -care_site_id))

                  } else if (self$visitMethod == "majority") {
                    return(\(x) dplyr::count(x,
                                             cohort_definition_id,
                                             subject_id,
                                             cohort_start_date,
                                             cohort_end_date,
                                             station) |>
                             dplyr::arrange(cohort_definition_id,
                                            subject_id,
                                            cohort_start_date,
                                            cohort_end_date,
                                            desc(n)) |>
                             dplyr::select(-n))
                  }
                }
)


HomeStation$set("public", "getHomeStation",
                function(){

                  self$cohort <- dplyr::tbl(self$connection,
                                            I(self$cohortTableName))


                  self$visitOccurrence <- dplyr::tbl(self$connection,
                                                     I(paste0(self$cdmSchema,
                                                              ".visit_occurrence"))) |>
                    dplyr::select(person_id,
                                  visit_occurrence_id,
                                  visit_start_date,
                                  care_site_id) |>
                    dplyr::rename(subject_id = person_id) |>
                    dplyr::filter(care_site_id != 0)


                  self$careSite <- dplyr::tbl(self$connection,
                                              I(paste0(self$cdmSchema,
                                                       ".care_site"))) |>
                    dplyr::select(care_site_id, x_institutioncode)


                  rankCareSite <- private$rankingFunction()


                  dplyr::left_join(self$cohort,
                                   self$visitOccurrence) |>
                    dplyr::filter(between(visit_start_date,
                                          sql(sprintf("DATEADD(year, %s, cohort_start_date)",
                                                      self$preDays*-1)),
                                          cohort_start_date)) |>
                    dplyr::distinct(cohort_definition_id,
                             subject_id,
                             visit_start_date,
                             .keep_all = TRUE) |>
                    dplyr::left_join(self$careSite) |>
                    dplyr::rename(station = x_institutioncode) |>
                    rankCareSite() |>
                    dplyr::distinct(cohort_definition_id,
                                    subject_id,
                                    cohort_start_date,
                                    .keep_all = TRUE) |>
                    dplyr::collect() -> self$homeStation

                  invisible(self)

                })

