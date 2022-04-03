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

# TODO: Add documentation for writer Monad for auditing functions
# TODO: Push documentation of writer Monad to list
# TODO: Push individual SQL queries as list
# TODO: Remove old code from here
