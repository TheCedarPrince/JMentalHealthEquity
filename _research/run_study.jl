using DrWatson
@quickactivate "MentalHealthEquity"

using CSV
using DrWatson
using DataFrames
using FunSQL: From, Get, Group, render
using LibPQ
using MentalHealthEquity


schema_info = introspect(conn; schema = :synpuf5)

for (idx, tbl) in enumerate(propertynames(schema_info) |> collect)
	@eval global $(Symbol(tbl)) = schema_info[$(idx)]
end

###############################
# DEFINE PATIENT VISITS
###############################

inpatient_codes = get_atlas_concept_set(
           read(datadir("exp_raw", "queries", "inpatient_cohort.json"), String),
           )

outpatient_codes = get_atlas_concept_set(read(datadir("exp_raw", "queries", "outpatient_cohort.json"), String))

visit_codes = [inpatient_codes, outpatient_codes, nothing]

###############################
# DEFINE PATIENT CONDITIONS
###############################

bipolar_df = CSV.read(
    datadir("exp_raw", "concept_sets", "bipolar_disorder_concept_set.csv"),
    DataFrame,
)

suicidality_df =
    CSV.read(datadir("exp_raw", "concept_sets", "suicidality_concept_set.csv"), DataFrame)

depression_df =
    CSV.read(datadir("exp_raw", "concept_sets", "depression_concept_set.csv"), DataFrame)

condition_codes =
    [bipolar_df.CONCEPT_ID, depression_df.CONCEPT_ID, suicidality_df.CONCEPT_ID, nothing]

###############################
# DEFINE PATIENT RACES
###############################

races =
    From(person) |>
    Group(Get.race_concept_id) |>
    render |>
    x -> LibPQ.execute(conn, x) |> DataFrame

race_codes = vcat(races.race_concept_id, nothing)

###############################
# DEFINE PATIENT AGE GROUPS
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
    nothing,
]

###############################
# DEFINE PATIENT GENDERS
###############################

genders =
    From(person) |>
    Group(Get.gender_source_value) |>
    render |>
    x -> LibPQ.execute(conn, x) |> DataFrame

gender_codes = vcat(genders.gender_source_value, nothing) |> vals -> filter(x -> !ismissing(x), vals)

###############################
# RUN THE STUDY
###############################

cohort_defs = collect(
    Iterators.product(visit_codes, condition_codes, gender_codes, race_codes, age_groups),
);

cohort_ids = []

counter = 1
for (visit, condition, gender, race, age_group) in cohort_defs

    ids = GenerateCohorts(
	conn;
        visit_codes = visit,
        condition_codes = condition,
        gender_codes = gender,
        race_codes = race,
        age_groupings = [age_group],
    )
    push!(cohort_ids, ids)

    println("Current cohort: $counter")
    counter += 1

end

populations = []

counter = 1
for (idx, cohort) in enumerate(cohort_ids)

    definition = cohort_defs[idx]

    df = GenerateStudyPopulations(
        cohort,
        conn;
        # by_visit = isnothing(definition[1]) ? false : true,
        by_state = isnothing(definition[2]) ? false : true,
        by_gender = isnothing(definition[3]) ? false : true,
        by_race = isnothing(definition[4]) ? false : true,
        by_age_group = isnothing(definition[5]) ? false : true,
    )

    push!(populations, df)

    println("Current population: $counter")
    counter += 1

end

study_pop = []
for i in 1:3

idx = findall(def -> 
# Find by visit type
def[1] == visit_codes[i] && 
# Find by condition type
def[2] == suicidality_df.CONCEPT_ID && 
# Find by gender
def[3] == nothing && 
# Find by race
def[4] == nothing && 
# Find by age group
def[5] == nothing, 
cohort_defs)

cohort_def = cohort_defs[idx][1]

cohort = GenerateCohorts(
    conn;
    visit_codes = cohort_def[1],
    condition_codes = cohort_def[2] |> x -> convert(Vector{Int}, x),
    gender_codes = cohort_def[3],
    race_codes = cohort_def[4],
    age_groupings = [cohort_def[5]],
)

df = GenerateStudyPopulations(cohort, conn; by_race = true, by_age_group = true)

push!(study_pop, df)

end
