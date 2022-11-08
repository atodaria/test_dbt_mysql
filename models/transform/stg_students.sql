
{{ config(materialized='table') }}

with source_data as (

    select 
	sd.student_id,
	sd.first_name, 
	sd.last_name, 
	sd.building, 
	sd.school_district,
	sd.grade,
	sd.gender,
	sd.language,
	sd.dob
	from google_sheets.students_data as sd

)

select *
from source_data
