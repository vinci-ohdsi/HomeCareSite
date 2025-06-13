#' Creates settings object
#'
#' @param connectionDetails connection details for DatabaseConnector
#' @param con database connection
#' @param cohortSchema schema containing the cohort table
#' @param cohortTable name of the cohort table
#' @param cdmSchema schema of the OMOP CDM
#' @param method character of length 1, how to derive home care site ("mostRecent", "mostFrequent")
#' @param careSiteColumn name of column in care_site table to use, default is standard "care_site_name"
#'
#' @returns list of class "homeCareSiteSettings"
#' @export
#'
#' @examples
createHomeCareSiteSettings <- function(connectionDetails = NULL,
                                      con = NULL,
                                      cohortSchema,
                                      cohortTable,
                                      cdmSchema,
                                      method = NULL,
                                      careSiteColumn = "care_site_name") {

  checkmate::assertClass(connectionDetails, "ConnectionDetails", null.ok = TRUE)
  checkmate::assertClass(con, "DatabaseConnectorJdbcConnection", null.ok = TRUE)
    
  if (is.null(connectionDetails) & is.null(connectionDetails)) {
    stop("Must provide either connectionDetails or connection")
  }
    

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
#' @returns list of class "homeSiteServerTable"
#' @export
#'
#' @examples
deriveHomeCareSite <- function(settings) {

  checkmate::assertClass(settings$connectionDetails, "ConnectionDetails", null.ok = TRUE)
  checkmate::assertClass(settings$con, "DatabaseConnectorJdbcConnection", null.ok = TRUE)

  start <-  Sys.time()

  if (is.null(settings$con) & is.null(settings$connectionDetails)) {
    stop("con and connectionDetails parameters cannot both be NULL")
  }

  if (is.null(settings$con)) {
    con <- DatabaseConnector::connect(settings$connectionDetails)
  } else {
    con <- settings$con
  }

  on.exit(DatabaseConnector::disconnect(con))

  templateSql <- SqlRender::readSql(system.file("sql/mostRecent.sql",
                                        package = "HomeCareSite",
                                        mustWork = TRUE))

  careSiteTableName <- paste0(settings$cohortTable,
                              "_care_site")

  renderedSql <- SqlRender::render(templateSql,
                                   cohort_schema = settings$cohortSchema,
                                   cohort_table = settings$cohortTable,
                                   cohort_table_new = careSiteTableName,
                                   cdm_schema = settings$cdmSchema,
                                   care_site_column = settings$careSiteColumn)


  translatedSql <- SqlRender::translate(renderedSql,
                                        settings$connectionDetails$dbms)

  DatabaseConnector::executeSql(
    con,
    translatedSql,
    progressBar = FALSE,
    reportOverallTime = FALSE
  )

  delta <- Sys.time() - start
  rlang::inform(paste0("Deriving home care site took ",
                       round(delta, 2),
                       attr(delta, "units")))

  tableReference <- list(connectionDetails = settings$connectionDetails,
                         cohortSchema = settings$cohortSchema,
                         careSiteTableName = careSiteTableName)

  class(tableReference) <- "homeCareSiteServerTable"

  return(tableReference)

}


#' Download derived home care site table from server
#'
#' @param settings object of class "homeCareSiteServerTable"
#'
#' @returns data frame
#' @export
#'
#' @examples
downloadHomeCareSite <- function(tableReference) {

  checkmate::assertClass(tableReference, "homeCareSiteServerTable")

  con <- DatabaseConnector::connect(tableReference$connectionDetails)

  on.exit(DatabaseConnector::disconnect(con))

  renderedSql <- SqlRender::render("select * from @schema.@table",
                              schema = tableReference$cohortSchema,
                              table = tableReference$careSiteTableName)

  translatedSql <- SqlRender::translate(renderedSql,
                                        tableReference$connectionDetails$dbms)

  result <- DatabaseConnector::querySql(con,
                                        translatedSql)

  return(result)

}


#' Summary generic method for class "homeCareSiteServerTable"
#'
#' @param tableReference list of class "homeCareSiteServerTable"
#'
#' @returns list of summary data
#' @export
#'
#' @examples
summary.homeCareSiteServerTable <- function(tableReference) {

  checkmate::assertClass(tableReference, "homeCareSiteServerTable")
  con <- DatabaseConnector::connect(tableReference$connectionDetails)
  on.exit(DatabaseConnector::disconnect(con))

  renderedSql <- SqlRender::render("select homeCareSite, count(homeCareSite) n
                                        from @schema.@table
                                        group by homeCareSite",
                                   schema = tableReference$cohortSchema,
                                   table = tableReference$careSiteTableName)

  translatedSql <- SqlRender::translate(renderedSql,
                                        tableReference$connectionDetails$dbms)

  results <- list()

  results$freq <- DatabaseConnector::querySql(con,
                                              translatedSql)

  return(results)

  }
