using DrWatson
@quickactivate "MentalHealthEquity"

using CSV
using DataFrames
using DBInterface
using LibPQ
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

inpatient_codes = get_atlas_concept_set(
    read(datadir("exp_raw", "queries", "inpatient_cohort.json"), String),
)

bipolar_df = CSV.read(
    datadir("exp_raw", "concept_sets", "bipolar_disorder_concept_set.csv"),
    DataFrame,
)

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

function VisitFilterPersonIDs(;visit_codes, tab = visit_occurrence)
    ids = From(tab) |> Where(Fun.in(Get.visit_concept_id, visit_codes...)) |> Group(Get.person_id) |> render |> x -> LibPQ.execute(conn, x) |> DataFrame

    return ids.person_id

end

function ConditionFilterPersonIDs(;condition_codes, tab = condition_occurrence)
    ids = From(tab) |> Where(Fun.in(Get.condition_concept_id, condition_codes...)) |> Group(Get.person_id) |> render |> x -> LibPQ.execute(conn, x) |> DataFrame

    return ids.person_id

end

function IntersectPersonIDs(;id_list)
	
	ids = intersect(id_list...)

	return ids

end

function GetPatientAgeGroup(;
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

# Separately calculate difficult queries and store them in Dictionaries

GetPatientAgeGroup
GetPatientLocation
GetPatientVisits

# Filter the database based on constraints to get granular necessary cohorts

VisitFilterPersonIDs
ConditionFilterPersonIDs
RaceFilterPersonIDs
AgeGroupFilterPersonIDs

# Find intersecting ids across constraints to generate final cohort

IntersectPersonIDs

# Group and compute counts or other necessary statistics



function run_study(;
    input_table = person,
    visit_codes = nothing,
)

    query = From(input_table)

    filter_list = []
    characteristics = Dict()

    !isnothing(visit_codes) && push!(filter_list, VisitFilterPersonIDs(; visit_codes = visit_codes))
    !isnothing(condition_codes) && push!(filter_list, ConditionFilterPersonIDs(; condition_codes = condition_codes)) # TODO
    !isnothing(race_codes) && push!(filter_list, RaceFilterPersonIDs(; race_codes = race_codes)) # TODO
    !isnothing(location_codes) && push!(filter_list, LocationFilterPersonIDs(; location_codes = location_codes)) # TODO
    !isnothing(age_groupings) && push!(filter_list, AgeGroupFilterPersonIDs(; age_grouping = age_grouping))
    !isnothing(gender_codes) && push!(filter_list, GenderGroupFilterPersonIDs(; gender_codes = gender_codes)) # TODO
    # Age group filtering will also calculate age but will not be used in getting the age group result
    
    cohort_ids = IntersectPersonIDs(filter_list)
    
    by_location && push!(characteristics, :location => GetPatientLocation) # TODO
    by_gender && push!(characteristics, :gender => GetPatientGender) # TODO
    by_race && push!(characteristics, :race => GetPatientRace) # TODO
    by_age_group && push!(characteristics, :age_group => GetPatientAgeGroup) # TODO

    # TODO 
    df = DataFrame(:person_id => cohort_ids)
    for feature in characteristics 
	Join(feature, df, on = :person_id => :person_id)    	
    end
    	
    # TODO
    From(df) |> Group([Get everything but person_id]) |> Select(Agg.count())

    return df

end

