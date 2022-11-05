using CSV
using DataFrames
using FunSQL
using FunSQL:
    SQLTable,
    Agg,
    As,
    Define,
    From,
    Fun,
    Get,
    Group,
    Join,
    Order,
    Select,
    WithExternal,
    Where,
    render,
    Limit,
    ID,
    LeftJoin,
    reflect
using LibPQ
using MentalHealthEquity
# TODO: Add the following packages to be used: OMOPCDMCohortCreator, OMOPCDMCDatabaseConnector, HealthSampleData

# FIX: Need to make connection to Eunomia

conn = DBInterface.connect(LibPQ.Connection, "") 

dialect = :postgresql

# TODO: Replace with OMOPCDMCohortCreator
MentalHealthEquity.GenerateConnectionDetails(conn,
    dialect = dialect,
    schema = "synpuf5"
)

# TODO: Add GenerateTables (I forget exact name)

###############################
# DEFINE PATIENT VISITS
###############################

inpatient_codes = MentalHealthEquity.get_atlas_concept_set(
           read("data/exp_raw/queries/inpatient_cohort.json", String),
           )

outpatient_codes = MentalHealthEquity.get_atlas_concept_set(read("data/exp_raw/queries/outpatient_cohort.json", String))

visit_codes = [inpatient_codes, outpatient_codes, nothing]

###############################
# DEFINE PATIENT CONDITIONS
###############################

bipolar_df = CSV.read(
    "data/exp_raw/concept_sets/bipolar_disorder_concept_set.csv",
    DataFrames.DataFrame,
)

suicidality_df =
    CSV.read("data/exp_raw/concept_sets/suicidality_concept_set.csv", DataFrames.DataFrame)

depression_df =
    CSV.read("data/exp_raw/concept_sets/depression_concept_set.csv", DataFrames.DataFrame)

condition_codes =
    [bipolar_df.CONCEPT_ID, depression_df.CONCEPT_ID, suicidality_df.CONCEPT_ID, nothing]

###############################
# DEFINE CONSTANTS
###############################

age_groups = [
    [0, 9],
    [10, 19],
    [20, 29],
    [30, 39],
    [40, 49],
    [50, 59],
    [60, 69],
    [70, 79],
    [80, 89],
]

###############################
# RUN THE STUDY
###############################

# TODO: Filter patients out by condition

# TODO: Get filtered patients' gender

# TODO: Get filtered patients' race

# TODO: Get filtered patients' age_group

# TODO: Join condition, race, gender, and age_group dataframes together

# TODO: Use the ExecuteAudit feature

# TODO: Write final dataframe per each condition out to csv
