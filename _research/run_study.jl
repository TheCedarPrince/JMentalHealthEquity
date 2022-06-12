using CSV
using DBInterface
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

conn = DBInterface.connect(LibPQ.Connection, "") 

dialect = :postgresql

MentalHealthEquity.GenerateConnectionDetails(conn,
    dialect = dialect,
    schema = "synpuf5"
)

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
# DEFINE PATIENT RACES
###############################

tab_info = FunSQL.reflect(conn, schema = "synpuf5")
person = tab_info[:person]

races =
    FunSQL.From(person) |>
    FunSQL.Group(FunSQL.Get.race_concept_id) |>
    q -> FunSQL.render(q, dialect = dialect) |>
    x -> LibPQ.execute(conn, x) |> DataFrames.DataFrame

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
    FunSQL.From(person) |>
    FunSQL.Group(FunSQL.Get.gender_concept_id) |>
    q -> FunSQL.render(q, dialect = dialect) |>
    x -> DBInterface.execute(conn, String(x)) |> DataFrames.DataFrame

gender_codes = vcat(genders.gender_concept_id, nothing) |> vals -> filter(x -> !ismissing(x), vals)

###############################
# RUN THE STUDY
###############################

cohort_defs = collect(
    Iterators.product(visit_codes, condition_codes, gender_codes, race_codes, age_groups),
);

cohort_ids = []

counter = 1
for (visit, condition, gender, race, age_group) in cohort_defs

    ids = MentalHealthEquity.GenerateCohorts(
	conn;
        visit_codes = visit,
        condition_codes = condition |> x -> convert(Vector{Int}, x),
        gender_codes = gender,
        race_codes = race,
        age_groupings = [age_group],
    )
    push!(cohort_ids, ids)

    break

end

populations = []

for (idx, cohort) in enumerate(cohort_ids)

    definition = cohort_defs[idx]

    df = MentalHealthEquity.GenerateStudyPopulations(
        cohort,
        conn;
        # by_visit = isnothing(definition[1]) ? false : true,
        by_state = isnothing(definition[2]) ? false : true,
        by_gender = isnothing(definition[3]) ? false : true,
        by_race = isnothing(definition[4]) ? false : true,
        by_age_group = isnothing(definition[5]) ? false : true,
    )

    push!(populations, df)

    break

end

# study_pop = []
# for i in 1:3

# idx = findall(def -> 
# # Find by visit type
# def[1] == visit_codes[i] && 
# # Find by condition type
# def[2] == suicidality_df.CONCEPT_ID && 
# # Find by gender
# def[3] == nothing && 
# # Find by race
# def[4] == nothing && 
# # Find by age group
# def[5] == nothing, 
# cohort_defs)

# cohort_def = cohort_defs[idx][1]

# cohort = GenerateCohorts(
    # conn;
    # visit_codes = cohort_def[1],
    # condition_codes = cohort_def[2] |> x -> convert(Vector{Int}, x),
    # gender_codes = cohort_def[3],
    # race_codes = cohort_def[4],
    # age_groupings = [cohort_def[5]],
# )

# df = GenerateStudyPopulations(cohort, conn; by_race = true, by_age_group = true)

# push!(study_pop, df)

# end
