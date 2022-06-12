###################################################################
# CREATE COHORT DEFINITION
###################################################################

idx = findall(
    def ->
    # Find patients with inpatient visits
        def[1] == inpatient_codes &&
            # Find patients with a condition event involving Bipolar Disorder
            def[2] == bipolar_df.CONCEPT_ID &&
            # Find patients who are male
            def[3] == 8507 &&
            # Do not filter by race
            def[4] == nothing &&
            # Do not filter by age group
            def[5] == nothing,
    # All possible cohorts previously calculated
    cohort_defs,
);

cohort_def = cohort_defs[idx][1];

###################################################################
# GENERATE COHORT
###################################################################

cohort = GenerateCohorts(
    conn;
    visit_codes = cohort_def[1],
    condition_codes = cohort_def[2] |> x -> convert(Vector{Int}, x),
    gender_codes = cohort_def[3],
    race_codes = cohort_def[4],
    age_groupings = [cohort_def[5]],
)

###################################################################
# FILTER TO LOSS TO FOLLOW-UP SUBCOHORT
###################################################################

cohort_recent_visits =
    MentalHealthEquity.GetMostRecentVisit(cohort, conn; tab = MentalHealthEquity.visit_occurrence)
cohort_recent_conditions =
    MentalHealthEquity.GetMostRecentConditions(cohort, conn; tab = MentalHealthEquity.condition_occurrence)

cohort_visit_conditions = MentalHealthEquity.GetVisitCondition(
    cohort_recent_visits.visit_occurrence_id |> x -> convert(Vector{Int}, x),
    conn;
    tab = MentalHealthEquity.condition_occurrence,
)
cohort_recent_visit_conditions = outerjoin(cohort_recent_visits, cohort_visit_conditions, on = :visit_occurrence_id => :visit_occurrence_id)

# THE ASSUMPTION HERE IS THAT THE MOST RECENT RECORDED CONDITIONS ARE BASED ON THE LAST REPORTED CONDITION AND VISIT OCCURRENCE COMBINED TOGETHER

cohort_recent_conditions = outerjoin(
    cohort_recent_visit_conditions,
    cohort_recent_conditions,
    on = [:person_id => :person_id, :condition_concept_id => :condition_concept_id],
    matchmissing = :equal,
    makeunique = true,
)[
    !,
    [:person_id, :condition_concept_id],
] |> unique

follow_up_cohort = []
for gdf in groupby(cohort_recent_conditions, :person_id)
	summed_truth = in(skipmissing(gdf.condition_concept_id)).(bipolar_df.CONCEPT_ID) |> sum
	println(summed_truth)
	if summed_truth >= 1
		push!(follow_up_cohort, gdf.person_id[1])
	end
end

###################################################################
# GENERATE COHORT DATA WITH GIVEN STRATIFICATIONS
###################################################################

cohort_data = GenerateStudyPopulations(
    follow_up_cohort |> x -> convert(Vector{Int}, follow_up_cohort),
    conn;
    by_visit = false,
    by_state = true,
    by_gender = false,
    by_race = false,
    by_age_group = true,
)

###################################################################
# GENERATE SUBPOPULATION GROUP COUNTS
###################################################################

subpop = GenerateGroupCounts(cohort_data)
