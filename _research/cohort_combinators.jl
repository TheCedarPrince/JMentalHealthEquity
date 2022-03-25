using DrWatson
@quickactivate "MentalHealthEquity"

using CSV
using DataFrames
using DBInterface
using LibPQ
using Memoization
using MentalHealthEquity
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
    LeftJoin
using Term.progress

schema_info = introspect(conn; schema = :synpuf5)

for (idx, tbl) in enumerate(propertynames(schema_info) |> collect)
	@eval $(Symbol(tbl)) = schema_info[$(idx)]
end
	
function PatientVisitFilter(;query, visit_codes, tab = visit_occurrence)

    query |> Where(Fun.in(Get.visit_concept_id, visit_codes...))

end

function PatientConditionFilter(;query, condition_codes, tab = condition_occurrence)

    query |> Where(Fun.in(Get.condition_concept_id, condition_codes...))

end

function PatientAgeGroup(
    ;query,
    initial_date = :year_of_birth,
    final_date = :observation_period_end_date,
    age_groupings = [
        [0, 9],
        [10, 19],
        [20, 29],
        [30, 39],
        [40, 49],
        [50, 59],
        [60, 69],
        [70, 79],
        [80, 89],
    ],
)

    age_arr = []
    for grp in age_groupings
        push!(age_arr, Get.age .< grp[2] + 1)
        push!(age_arr, "$(grp[1]) - $(grp[2])")
    end

        query |>
        Define(
            :age => Fun.date_part(
                "year",
                Fun.age(Get[final_date], Fun.make_date(Get[initial_date], 1, 1)),
            ),
        ) |>
        Define(:age_group => Fun.case(age_arr...))

end

function JoinOnColumn(;query, join_table, column = :person_id, over = nothing)

    columns = setdiff(join_table.column_set, column_logger) |> collect
    push!(column_logger, columns...)
    push!(table_logger, [join_table.name, columns])

    if isnothing(over)
    query |>
    Join(
	From(join_table),
        on = Get[over][column] .== Get[column],
    ) |> return
    end
    query |>
    Join(
	From(join_table),
        on = Get[over][column] .== Get[column],
    )

end

function SelectAllColumns(query)
 columns = []
 for idx in 1:length(table_logger)
     get_names = []
         for group in table_logger[end:-1:idx]
             push!(get_names, group[1])
         end
     group_get = Get(get_names[1])
     if length(get_names) != 1
	for name in get_names[2:end]
		group_get = group_get |> Get(name)
	end
     end
     push!(columns, [group_get[x] for x in table_logger[idx][2]]...)
 end

    query |> Select(columns...)
end

# INITIAL PROTOTYPE
# Works, but is extremely slow

function generate_statistics(;
    input_table = person,
    visit_codes = nothing,
    condition_codes = nothing,
    by_sex = false,
    by_age = false,
    by_race = false,
    by_care_site = false,
    by_location = false,
    by_person = true, 
    by_count = true,
    limit_to = nothing,
)

    global table_logger = [[input_table.name, collect(input_table.column_set)]]
    global column_logger = [collect(input_table.column_set)...]

    query = From(input_table)

    if !isnothing(limit_to)
    query = From(input_table) |> Limit(limit_to) |> As(input_table.name)
    else
    query = From(input_table) |> As(input_table.name)
    end

    table_name = input_table.name

    if !isnothing(visit_codes)
        query =
            JoinOnColumn(
                query = query,
                join_table = visit_occurrence,
                over = table_name,
            ) |>
            x -> PatientVisitFilter(query = x, visit_codes = visit_codes) |>
            As(:visit_occurrence)
        table_name = :visit_occurrence
    end

    if !isnothing(condition_codes)
        query =
            JoinOnColumn(
                query = query,
                join_table = condition_occurrence,
                over = table_name,
            ) |> As(:condition_occurrence)
        table_name = :condition_occurrence
    end

    if by_age == true
        query =
                JoinOnColumn(query = query, join_table = observation_period, over = table_name) |>
                As(:observation_period)
        table_name = :observation_period
    end

    fields = []

    by_person && push!(fields, Get.person_id)
    by_sex && push!(fields, Get.gender_source_value)
    by_age && push!(fields, Get.age_group)
    by_race && push!(fields, Get.race_source_value)
    by_care_site && push!(fields, Get.care_site_id)
    by_location && push!(fields, Get.location_id)

    query = query |> SelectAllColumns

    if by_age == true
	query = PatientAgeGroup(query = query)
    end
    println(fields)
    query

    # if by_count == true
    # query |> Group(fields...) |> Select(fields..., Agg.count())
    # else
    # query |> Group(fields...)
    # end

end


generate_statistics(
    # visit_codes = inpatient_codes,
    # condition_codes = bipolar_df.CONCEPT_ID,
    limit_to = 10,
    by_count = false,
    by_sex = true,
    by_race = true,
    # by_age = true,
) |>
Group([Get.person_id, Get.gender_source_value, Get.race_source_value, Get.year_of_birth]...) |>
Select([Get.person_id, Get.gender_source_value, Get.race_source_value, Get.year_of_birth]...) |> 
# Group([Get.gender_source_value, Get.race_source_value]...) |>
# Select([Get.gender_source_value, Get.race_source_value]..., Agg.count()) |> 
render |>
x -> LibPQ.execute(conn, x) |> DataFrame


function GetDatabasePersonIDs(;tab = person)
    ids = From(tab) |> Group(Get.person_id) |> render |> x -> LibPQ.execute(conn, x) |> DataFrame

    return ids.person_id

end

@memoize Dict function VisitFilterPersonIDs(; visit_codes, tab = visit_occurrence)
    ids =
        From(tab) |>
        Where(Fun.in(Get.visit_concept_id, visit_codes...)) |>
        Group(Get.person_id) |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

    return ids.person_id

end

@memoize Dict function ConditionFilterPersonIDs(;
    condition_codes,
    tab = condition_occurrence,
)
    ids =
        From(tab) |>
        Where(Fun.in(Get.condition_concept_id, condition_codes...)) |>
        Group(Get.person_id) |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

    return ids.person_id

end

@memoize Dict function RaceFilterPersonIDs(; race_codes, tab = person)
    ids =
        From(tab) |>
        Where(Fun.in(Get.race_concept_id, race_codes...)) |>
        Group(Get.person_id) |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

    return ids.person_id

end

@memoize Dict function GenderFilterPersonIDs(; gender_codes, tab = person)
    ids =
        From(tab) |>
        Where(Fun.in(Get.gender_concept_id, gender_codes...)) |>
        Group(Get.person_id) |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

    return ids.person_id

end

@memoize Dict function StateFilterPersonIDs(; states, tab = location, join_tab = person)
    ids =
        From(tab) |>
        Select(Get.location_id, Get.state) |>
        Where(Fun.in(Get.state, states...)) |>
        Join(:join => join_tab, Get.location_id .== Get.join.location_id) |>
        Select(Get.join.person_id) |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

    return ids.person_id

end

@memoize Dict function GetPatientState(; ids, tab = location, join_tab = person)
    df =
        From(tab) |>
        Select(Get.location_id, Get.state) |>
        Join(:join => join_tab, Get.location_id .== Get.join.location_id) |>
        Where(Fun.in(Get.join.person_id, ids...)) |>
        Select(Get.join.person_id, Get.state) |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

    return df

end

@memoize Dict function GetPatientGender(; ids, tab = person)
    df =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.gender_concept_id) |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

    return df

end

@memoize Dict function GetPatientRace(; ids, tab = person)
    df =
        From(tab) |>
        Where(Fun.in(Get.person_id, ids...)) |>
        Select(Get.person_id, Get.race_concept_id) |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

    return df

end

function IntersectPersonIDs(; id_list)

    ids = intersect(id_list...)

    return ids

end

@memoize Dict function AgeGroupFilterPersonIDs(; age_groupings, tab = person)

    age_arr = []
    age_ranges = []
    for grp in age_groupings
        push!(age_arr, Get.age .< grp[2] + 1)
        push!(age_arr, "$(grp[1]) - $(grp[2])")
        push!(age_ranges, "$(grp[1]) - $(grp[2])")
    end

    ids =
        From(person) |>
        LeftJoin(
            :observation_group => From(observation_period) |> Group(Get.person_id),
            on = Get.person_id .== Get.observation_group.person_id,
        ) |>
        Select(
            Get.person_id,
            Fun.make_date(Get.year_of_birth, Get.month_of_birth, Get.day_of_birth) |>
            As(:dob),
            Get.observation_group |>
            Agg.max(Get.observation_period_end_date) |>
            As(:record),
        ) |>
        Select(
            Get.person_id,
            :age => Fun.date_part("year", Fun.age(Get.record, Get.dob)),
        ) |>
        Define(:age_group => Fun.case(age_arr...)) |>
        Where(Fun.in(Get.age_group, age_ranges...)) |>
        Select(Get.person_id) |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

    return ids.person_id

end

@memoize Dict function GetPatientAgeGroup(;
    ids,
    age_groupings = [
        [0, 9],
        [10, 19],
        [20, 29],
        [30, 39],
        [40, 49],
        [50, 59],
        [60, 69],
        [70, 79],
        [80, 89],
    ],
)

    age_arr = []
    for grp in age_groupings
        push!(age_arr, Get.age .< grp[2] + 1)
        push!(age_arr, "$(grp[1]) - $(grp[2])")
    end

    From(person) |>
    Where(Fun.in(Get.person_id, ids...)) |>
    LeftJoin(
        :observation_group => From(observation_period) |> Group(Get.person_id),
        on = Get.person_id .== Get.observation_group.person_id,
    ) |>
    Select(
        Get.person_id,
        Fun.make_date(Get.year_of_birth, Get.month_of_birth, Get.day_of_birth) |> As(:dob),
        Get.observation_group |> Agg.max(Get.observation_period_end_date) |> As(:record),
    ) |>
    Select(Get.person_id, :age => Fun.date_part("year", Fun.age(Get.record, Get.dob))) |>
    Define(:age_group => Fun.case(age_arr...)) |>
    Select(Get.person_id, Get.age_group) |>
    render |>
    x -> LibPQ.execute(conn, x) |> DataFrame

end

function generate_cohort(;
    visit_codes = nothing,
    condition_codes = nothing,
    race_codes = nothing,
    location_codes = nothing,
    gender_codes = nothing,
    age_groupings = nothing,
)
    filter_list = []

    !isnothing(visit_codes) &&
        push!(filter_list, VisitFilterPersonIDs(; visit_codes = visit_codes))
    !isnothing(condition_codes) &&
        push!(filter_list, ConditionFilterPersonIDs(; condition_codes = condition_codes))
    !isnothing(race_codes) &&
        push!(filter_list, RaceFilterPersonIDs(; race_codes = race_codes))
    !isnothing(location_codes) &&
        push!(filter_list, StateFilterPersonIDs(; state_codes = state_codes))
    !isnothing(gender_codes) &&
        push!(filter_list, GenderFilterPersonIDs(; gender_codes = gender_codes))

    !isnothing(age_groupings[1]) &&
        push!(filter_list, AgeGroupFilterPersonIDs(; age_groupings = age_groupings))

    IntersectPersonIDs(id_list = filter_list)

end

function run_study(;
    cohort_ids,
    by_state = false,
    by_gender = false,
    by_race = false,
    by_age_group = false,
)

    characteristics = Dict()

    by_state && push!(characteristics, :state => GetPatientState(ids = cohort_ids))
    by_gender && push!(characteristics, :gender => GetPatientGender(ids = cohort_ids))
    by_race && push!(characteristics, :race => GetPatientRace(ids = cohort_ids))
    by_age_group &&
        push!(characteristics, :age_group => GetPatientAgeGroup(ids = cohort_ids))

    df = DataFrame(:person_id => cohort_ids)
    for feature in keys(characteristics)
        df = innerjoin(df, characteristics[feature], on = :person_id)
    end

    return df

end

function comply(; data, hitech = true)
    cols = filter(x -> x != :person_id, propertynames(data))
    df = groupby(data, cols) |> x -> combine(x, nrow => :count)

    hitech && filter!(row -> row.count >= 11, df)

    return df

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

gender_codes = [8507, 8532, nothing]

###############################
# RUN THE STUDY
###############################

cohorts = collect(
    Iterators.product(visit_codes, condition_codes, gender_codes, race_codes, age_groups),
);

test = []

counter = 1
for (visit, condition, gender, race, age_group) in cohorts

    ids = generate_cohort(
        visit_codes = visit,
        condition_codes = condition,
        gender_codes = gender,
        race_codes = race,
        age_groupings = [age_group],
    )
    push!(cohorts, ids)

    println("Current cohort: $counter")
    counter += 1

end

for (idx, cohort) in enumerate(cohort_ids)

definition = cohorts[idx]

run_study(;
    cohort,
    by_visit = isnothing(definition[1]) ? false : true, # TODO: Add this feature
    by_state = isnothing(definition[2]) ? false : true,
    by_gender = isnothing(definition[3]) ? false : true,
    by_race = isnothing(definition[4]) ? false : true,
    by_age_group = isnothing(definition[5]) ? false : true,
)

end

# TODO: Add documentation to functions
# TODO: Add documentation for writer Monad for auditing functions
# TODO: Push documentation of writer Monad to list
# TODO: Push individual SQL queries as list
# TODO: Move Filter functions to MentalHealthEquity package
# TODO: Move Get functions to MentalHealthEquity package
# TODO: Remove old code from here
