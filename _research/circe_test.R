library(CirceR)
library(CohortGenerator)
library(DatabaseConnector)

# First construct a cohort definition set: an empty 
# data frame with the cohorts to generate
cohortsToCreate <- CohortGenerator::createEmptyCohortDefinitionSet()

# Fill the cohort set using  cohorts included in this 
# package as an example
  cohortJsonFileName <- "depression_total.json"
  cohortName <- tools::file_path_sans_ext(basename(cohortJsonFileName))
  # Here we read in the JSON in order to create the SQL
  # using [CirceR](https://ohdsi.github.io/CirceR/)
  # If you have your JSON and SQL stored differenly, you can
  # modify this to read your JSON/SQL files however you require
  cohortJson <- readChar(cohortJsonFileName, file.info(cohortJsonFileName)$size)
  cohortExpression <- CirceR::cohortExpressionFromJson(cohortJson)
  cohortSql <- CirceR::buildCohortQuery(cohortExpression, options = CirceR::createGenerateOptions(generateStats = FALSE))
  cohortsToCreate <- rbind(cohortsToCreate, data.frame(cohortId = 1,
                                                       cohortName = cohortName, 
                                                       sql = cohortSql,
                                                       stringsAsFactors = FALSE))

# Generate the cohort set against Eunomia. 
# cohortsGenerated contains a list of the cohortIds 
# successfully generated against the CDM
connectionDetails <- DatabaseConnector::createConnectionDetails(
  dbms = "postgresql",
  server = "",
  user = "",
  password = "",
  port = ,
  pathToDriver = "utils"
)

# Create the cohort tables to hold the cohort generation results
cohortTableNames <- CohortGenerator::getCohortTableNames(cohortTable = "my_cohort_table")
CohortGenerator::createCohortTables(connectionDetails = connectionDetails,
                                                        cohortDatabaseSchema = "mimic_v531",
                                                        cohortTableNames = cohortTableNames)
# Generate the cohorts
cohortsGenerated <- CohortGenerator::generateCohortSet(connectionDetails = connectionDetails,
                                                       cdmDatabaseSchema = "mimic_v531",
                                                       cohortDatabaseSchema = "mimic_v531",
                                                       cohortTableNames = cohortTableNames,
                                                       cohortDefinitionSet = cohortsToCreate)

# Get the cohort counts
cohortCounts <- CohortGenerator::getCohortCounts(connectionDetails = connectionDetails,                                                 cohortDatabaseSchema = "mimic_v531",                                                 cohortTable = cohortTableNames$cohortTable)
print(cohortCounts)
