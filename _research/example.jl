###################################################################
# CREATE COHORT DEFINITION
###################################################################

idx = findall(def ->
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
       cohort_defs);

cohort_def = cohort_defs[idx][1];

###################################################################
# GENERATE COHORT 
###################################################################

cohort = 
    GenerateCohorts(
	conn; 
        visit_codes = cohort_def[1],
        condition_codes = cohort_def[2] |> x -> convert(Vector{Int}, x),
        gender_codes = cohort_def[3],
        race_codes = cohort_def[4],
        age_groupings = [cohort_def[5]],
    )

###################################################################
# GENERATE COHORT DATA WITH GIVEN STRATIFICATIONS
###################################################################

cohort_data = 
    GenerateStudyPopulations(
        cohort,
        conn;
	by_visit = false,
        by_state = true,
        by_gender = false,
        by_race = false,
        by_age_group = true
    )

###################################################################
# GENERATE SUBPOPULATION GROUP COUNTS
###################################################################

subpop = GenerateGroupCounts(cohort_data)
