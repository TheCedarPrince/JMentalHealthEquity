"""
TODO: Add docs when ready
"""
function GenerateCohorts(conn;
    visit_codes = nothing,
    condition_codes = nothing,
    race_codes = nothing,
    location_codes = nothing,
    gender_codes = nothing,
    age_groupings = nothing,
)
    filter_list = []

    !isnothing(visit_codes) &&
        push!(filter_list, VisitFilterPersonIDs(visit_codes, conn))
    !isnothing(condition_codes) &&
        push!(filter_list, ConditionFilterPersonIDs(condition_codes, conn))
    !isnothing(race_codes) &&
        push!(filter_list, RaceFilterPersonIDs(race_codes, conn))
    !isnothing(location_codes) &&
        push!(filter_list, StateFilterPersonIDs(state_codes, conn))
    !isnothing(gender_codes) &&
        push!(filter_list, GenderFilterPersonIDs(gender_codes, conn))

    !isnothing(age_groupings[1]) &&
        push!(filter_list, AgeGroupFilterPersonIDs(age_groupings, conn))

    isempty(filter_list) ? filter_list : convert(Vector{Int}, intersect(filter_list...))

end

"""
TODO: Add docs when ready
"""
function GenerateStudyPopulations(cohort_ids, conn;
    by_visit = false,
    by_state = false,
    by_gender = false,
    by_race = false,
    by_age_group = false,
)

    characteristics = Dict()

    by_visit && push!(characteristics, :visit => GetPatientVisits(cohort_ids, conn))
    by_state && push!(characteristics, :state => GetPatientState(cohort_ids, conn))
    by_gender && push!(characteristics, :gender => GetPatientGender(cohort_ids, conn))
    by_race && push!(characteristics, :race => GetPatientRace(cohort_ids, conn))
    by_age_group &&
        push!(characteristics, :age_group => GetPatientAgeGroup(cohort_ids, conn))

    df = DataFrame(:person_id => cohort_ids)
    for feature in keys(characteristics)
        df = innerjoin(df, characteristics[feature], on = :person_id)
    end

    return df

end

"""
TODO: Add docs when ready
"""
function GenerateGroupCounts(data::DataFrame)
    cols = filter(x -> x != :person_id, propertynames(data))
    df = groupby(data, cols) |> x -> combine(x, nrow => :count)

    return df

end

function GenerateConnectionDetails(conn;
    dialect = :postgresql,
    schema = nothing
)

    global dialect = dialect
    if dialect == :postgresql
        db_info = reflect(conn; schema = schema, dialect = dialect)

        for key in keys(db_info.tables)
            @eval global $(Symbol(key)) = $(db_info[key])
        end
    elseif dialect == :mysql
        db_info = reflect(conn; schema = schema, dialect = dialect)

        for key in keys(db_info.tables)
            @eval global $(Symbol(key)) = $(db_info[key])
        end

    end
        
    return conn

end

export GenerateCohorts, GenerateConnectionDetails, GenerateGroupCounts, GenerateStudyPopulations
