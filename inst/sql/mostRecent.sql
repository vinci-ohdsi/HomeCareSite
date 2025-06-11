
drop table if exists @cohort_schema.@cohort_table_new;

with visits as (
  select distinct cohort.*,
    condition.condition_start_date,
    condition.visit_occurrence_id,
    visit.visit_start_date,
    cs.@care_site_column,
    row_number() over (partition by cohort.cohort_definition_id,
                                      cohort.subject_id,
                                      cohort.cohort_start_date
                         order by visit.visit_start_date desc) sort_order

  from @cohort_schema.@cohort_table cohort

    left join @cdm_schema.condition_occurrence condition
      on cohort.subject_id = condition.person_id

    left join @cdm_schema.visit_occurrence visit
      on condition.visit_occurrence_id = visit.visit_occurrence_id

    left join @cdm_schema.care_site cs
      on visit.care_site_id = cs.care_site_id

  where visit.care_site_id is not null
    and visit.visit_start_date between DATEADD(day, -365, cohort.cohort_start_date) and cohort.cohort_start_date )

select cohort_definition_id,
    subject_id,
    cohort_start_date,
    cohort_end_date,
    @care_site_column homeCareSite
into @cohort_schema.@cohort_table_new
from visits
where sort_order = 1








