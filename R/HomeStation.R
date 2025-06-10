

#' Creates settings object
#'
#' @param connectionDetails
#' @param con
#' @param cohortSchema
#' @param cohortTable
#' @param cdmSchema
#' @param method
#'
#' @returns list of class "homeCareSiteSettings"
#' @export
#'
#' @examples
createHomeCareSiteSettings <- function(connectionDetails,
                                      con = NULL,
                                      cohortSchema,
                                      cohortTable,
                                      cdmSchema,
                                      method = NULL,
                                      careSiteColumn = "care_site_name") {

  checkmate::assertClass(connectionDetails, "ConnectionDetails")
  checkmate::assertClass(con, "DatabaseConnectorJdbcConnection")

  stopifnot(DatabaseConnector::existsTable(con, cohortSchema, cohortTable))

  settings <- list(connectionDetails = connectionDetails,
                   con = con,
                   cohortSchema = cohortSchema,
                   cohortTable = cohortTable,
                   cdmSchema = cdmSchema,
                   method = method,
                   careSiteColumn = careSiteColumn)

  class(settings) <- "homeCareSiteSettings"

  return(settings)

}


#' Derive home care site
#'
#' @param settings object of class "homeCareSiteSettings"
#'
#' @returns NULL invisibly, via side effect writes to server table
#' @export
#'
#' @examples
deriveHomeCareSite <- function(settings) {

  start <-  Sys.time()

  if (is.null(con)) {
    con <- DatabaseConnector::connect(settings$connectionDetails)
    on.exit(DatabaseConnector::disconnect(connection))
  }

  templateSql <- SqlRender::readSql(system.file("sql/mostRecent.sql",
                                        package = "HomeCareSite",
                                        mustWork = TRUE))


  renderedSql <- SqlRender::render(templateSql,
                                   cohort_schema = settings$cohortSchema,
                                   cohort_table = settings$cohortTable,
                                   cohort_table_new = paste0(settings$cohortTable,
                                                             "_care_site"),
                                   cdm_schema = settings$cdmSchema,
                                   care_site_column = settings$careSiteColumn)


  translatedSql <- SqlRender::translate(renderedSql,
                                        settings$connectionDetails$dbms)

  DatabaseConnector::executeSql(
    settings$con,
    translatedSql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  delta <- Sys.time() - start
  rlang::inform(paste0("Deriving home care site took ",
                       round(delta, 2),
                       attr(delta, "units")))

  return(invisible(NULL))

}
