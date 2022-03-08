using DrWatons
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
    ID

conn = LibPQ.Connection("host=data.hdap.gatech.edu port=5442 user=mimic_v531 password=i3lworks")   

inpatient_codes = get_atlas_concept_set(
    read(datadir("exp_raw", "queries", "inpatient_cohort.json"), String),
)

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
function PatientVisitFilter(q; visit_codes, tab = visit_occurrence)

    q |> Where(Fun.in(Get.visit_concept_id, visit_codes...))

end
function PatientConditionFilter(q; condition_codes, tab = condition_occurrence)

    q |> Where(Fun.in(Get.condition_concept_id, condition_codes...))

end
function PatientAgeGroup(
    q;
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

        q |>
        Define(
            :age => Fun.date_part(
                "year",
                Fun.age(Fun.to_date(Get[final_date], "YYYY-MM-DD"), Fun.make_date(Get[initial_date], 1, 1)),
            ),
        ) |>
        Define(:age_group => Fun.case(age_arr...))

end

function JoinOnColumn(q; join_table, column = :person_id, over = nothing)

    columns = setdiff(join_table.column_set, column_logger) |> collect
    push!(column_logger, columns...)
    push!(table_logger, [join_table.name, columns])

    if isnothing(over)
    q |>
    Join(
	From(join_table),
        on = Get[over][column] .== Get[column],
    ) |> return
    end
    q |>
    Join(
	From(join_table),
        on = Get[over][column] .== Get[column],
    )

end
function SelectAllColumns(q)
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

    q |> Select(columns...)
end

global table_logger = [[visit_occurrence.name, collect(visit_occurrence.column_set)]]
global column_logger = [collect(visit_occurrence.column_set)...]

df = From(visit_occurrence) |>
# Limit(1000) |> 
x -> PatientVisitFilter(x, visit_codes = inpatient_codes) |> 
As(:visit_occurrence) |>
x -> JoinOnColumn(x, join_table = person, over = :visit_occurrence) |>
As(:person) |>
x -> JoinOnColumn(x, join_table = condition_occurrence, over = :person) |>
x -> PatientConditionFilter(x, condition_codes = bipolar_df.CONCEPT_ID) |> 
As(:condition_occurrence) |> 
SelectAllColumns |> 
# Group(Get.person_id) |> 
Select(Get.person_id, Get.care_site_id, Get.location_id, Get.race_source_value, Get.gender_source_value, Get.condition_source_value) |> 
render |>
x -> LibPQ.execute(conn, x) # |>
DataFrame

global table_logger = [[person.name, collect(person.column_set)]]
global column_logger = [collect(person.column_set)...]

From(person) |>
Limit(z) |>
As(:person) |>
x -> JoinOnColumn(x, join_table = visit_occurrence, over = :person) |>
As(:visit_occurrence) |>
x -> JoinOnColumn(x, join_table = condition_occurrence, over = :visit_occurrence) |>
As(:condition_occurrence) |> 
# Select(Get.condition_occurrence.person.location_id) |> 
# Select(Get.condition_occurrence.person.visit_occurrence.visit_occurrence_id) |> 
SelectAllColumns |> 
render
