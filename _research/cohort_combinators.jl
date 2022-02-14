using DBInterface
using LibPQ
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
    Where,
    render,
    Limit

allergy_observation_concepts = schema_info[:allergy_observation_concepts]
attribute_definition = schema_info[:attribute_definition]
care_site = schema_info[:care_site]
cdm_source = schema_info[:cdm_source]
cohort = schema_info[:cohort]
cohort_attribute = schema_info[:cohort_attribute]
cohort_definition = schema_info[:cohort_definition]
concept = schema_info[:concept]
concept_ancestor = schema_info[:concept_ancestor]
concept_class = schema_info[:concept_class]
concept_relationship = schema_info[:concept_relationship]
concept_synonym = schema_info[:concept_synonym]
condition_era = schema_info[:condition_era]
condition_era_bkp = schema_info[:condition_era_bkp]
condition_occurrence = schema_info[:condition_occurrence]
condition_occurrence_bkp = schema_info[:condition_occurrence_bkp]
cost = schema_info[:cost]
death = schema_info[:death]
device_exposure = schema_info[:device_exposure]
device_exposure_bkp = schema_info[:device_exposure_bkp]
domain = schema_info[:domain]
dose_era = schema_info[:dose_era]
drug_era = schema_info[:drug_era]
drug_era_bkp = schema_info[:drug_era_bkp]
drug_exposure = schema_info[:drug_exposure]
drug_exposure_bkp = schema_info[:drug_exposure_bkp]
drug_strength = schema_info[:drug_strength]
fact_relationship = schema_info[:fact_relationship]
id_mapping = schema_info[:id_mapping]
location = schema_info[:location]
measurement = schema_info[:measurement]
metadata = schema_info[:metadata]
note = schema_info[:note]
note_nlp = schema_info[:note_nlp]
observation = schema_info[:observation]
observation_period = schema_info[:observation_period]
payer_plan_period = schema_info[:payer_plan_period]
person = schema_info[:person]
procedure_occurrence = schema_info[:procedure_occurrence]
procedure_occurrence_bkp = schema_info[:procedure_occurrence_bkp]
provider = schema_info[:provider]
relationship = schema_info[:relationship]
source_to_concept_map = schema_info[:source_to_concept_map]
specimen = schema_info[:specimen]
visit_detail = schema_info[:visit_detail]
visit_occurrence = schema_info[:visit_occurrence]
vocabulary = schema_info[:vocabulary]

function PatientVisitFilter(visit_codes; tab = visit_occurrence, features = nothing)

    q = From(tab) |> Where(Fun.in(Get.visit_concept_id, visit_codes...))

    if !isnothing(features)
        groups = [Get[f] for f in features]
        q = q |> Group(groups...)
    else
        return q
    end

end

function JoinOnColumn(input_table, join_table, input_cols; column = :person_id)

    #=

    NOTE: the kwarg `input_cols` is a hack
    It should really be a lot more generalized but right now, one must provide
    the columns for the input query.

    =#

    input_table |>
    Join(
        From(join_table) |> As(:join_table),
        on = Get.join_table[column] .== Get[column],
    ) |>
    Select(
        Get.(input_cols)...,
        Get.(filter(!in(input_cols), join_table.column_set), over = Get.join_table)...,
    )

end

function PatientLocation(tab; location_tab = location, features = nothing)

    q =
        tab |> Join(
            From(location_tab) |> As(:location_tab),
            on = Get.location_tab.location_id .== Get.location_id,
        )

    if !isnothing(features)
        groups = [Get[f] for f in features]
        q = q |> Group(groups...)
        return q
    else
        return q
    end

end

function PatientsByState(tab; location_tab = location, features = nothing)

    PatientLocation(tab; location_tab = location_tab, features = features) |>
    Group(Get.location_tab.state) |>
    Select(Get.state, Agg.count())

end

PatientVisitFilter(inpatient_codes) |>
Limit(100) |>
x -> JoinOnColumn(x, person, visit_occurrence.column_set) |> PatientsByState

function PatientAgeGroup(
    tab;
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

    q =
        tab |>
        Define(
            :age => Fun.date_part(
                "year",
                Fun.age(Get[final_date], Fun.make_date(Get[initial_date], 1, 1)),
            ),
        ) |>
        Define(:age_group => Fun.case(age_arr...))

    return q

end

function PatientsByAgeGroup(
    tab;
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

    PatientAgeGroup(
        tab;
        initial_date = initial_date,
        final_date = final_date,
        age_groupings = age_groupings,
    ) |>
    Group(Get.age_group) |>
    Select(Get.age_group, Agg.count())

end

PatientVisitFilter(inpatient_codes) |>
Limit(100) |>
x ->
    JoinOnColumn(x, observation_period, visit_occurrence.column_set) |>
    x ->
        JoinOnColumn(
            x,
            person,
            union(visit_occurrence.column_set, observation_period.column_set),
        ) |>
        PatientsByAgeGroup |>
        render |>
        x -> LibPQ.execute(conn, x) |> DataFrame

function PatientsByRace(tab)

    tab |> Group(Get.race_concept_id) |> Select(Get.race_concept_id, Agg.count())

end


PatientVisitFilter(inpatient_codes) |>
Limit(100) |>
x -> JoinOnColumn(x, person, visit_occurrence.column_set) |>
x -> JoinOnColumn(x, location, union(visit_occurrence.column_set, person.column_set); column = :location_id) |>
PatientsByRace |>
render |>
x -> LibPQ.execute(conn, x) |> DataFrame

function PatientsByGender(tab)

    tab |> Group(Get.gender_concept_id) |> Select(Get.gender_concept_id, Agg.count())

end

PatientVisitFilter(inpatient_codes) |>
Limit(100) |>
x -> JoinOnColumn(x, person, visit_occurrence.column_set) |>
PatientsByGender |>
render |>
x -> LibPQ.execute(conn, x) |> DataFrame



function PatientsByCondition(tab)

    tab |> Group(Get.condition_concept_id) |> Select(Get.condition_concept_id, Agg.count())

end

PatientVisitFilter(inpatient_codes) |>
Limit(100) |>
x -> JoinOnColumn(x, condition_occurrence, visit_occurrence.column_set) |>
PatientsByCondition |>
render |>
x -> LibPQ.execute(conn, x) |> DataFrame

function PatientsByCareSite(tab)

    tab |> Group(Get.place_of_service_concept_id) |> Select(Get.place_of_service_concept_id, Agg.count())

end

PatientVisitFilter(inpatient_codes) |>
Limit(100) |>
x -> JoinOnColumn(x, care_site, visit_occurrence.column_set; column = :care_site_id) |>
PatientsByCareSite |>
render |>
x -> LibPQ.execute(conn, x) |> DataFrame

PatientVisitFilter(inpatient_codes) |> 
