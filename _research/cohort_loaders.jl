using DrWatson
@quickactivate "MentalHealthEquity"

using DBInterface
using LibPQ
using OHDSICohortExpressions: translate, Model

cohort = read("_research/depression_total.json", String)

model = Model(
    cdm_version = v"5.3.1",
    cdm_schema = "synpuf5",
    vocabulary_schema = "synpuf5",
    results_schema = "synpuf5",
    target_schema = "synpuf5",
    target_table = "my_cohort_table",
);

tsql = translate(cohort, dialect = :postgresql, model = model, cohort_definition_id = 1);

LibPQ.execute(conn, tsql)


