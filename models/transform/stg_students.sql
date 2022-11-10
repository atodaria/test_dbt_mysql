
{{ config(materialized='table') }}


with cte_students as (
select distinct sd.`Student ID` as student_id,
		sd.`First Name` as first_name,
		sd.`Last Name` as last_name,
        sd.`gender` as gender,
		sd.`dob` as dob,
		sd.`discipline` as discipline,      
		sd.`School District` as school_district,
        sd.`building` as building,
        sd.`grade` as grade,
        sd.`language` as language,		        
		row_number() over(partition by sd.`Student ID`,sd.`building` order by sd.`Student ID`) as rownum,  
        sd.`_airbyte_normalized_at` as event_time
from testdataimport.students_import_data as sd
left join portal.students as s on sd.`Student ID` = s.student_id
and sd.`First Name` = s.first_name
and sd.`Last Name` = s.last_name
where s.student_id is null
and 
(
not exists (select 1 from {{ this }})     
or sd.`_airbyte_normalized_at` > (select max(`updated_at`) from {{ this }})
)
)

select     
	sd.student_id,
    sd.first_name,
	sd.last_name,	
    d.id as district_id,
    scl.id as school_id,
    null as icdCode_id,
    grd.id as grade_id,
    l.id as primaryLanguage_id,
    null as contact_phone_number,
    null as contact_email,
    STR_TO_DATE(sd.dob, '%m/%d/%Y %T') as birth_date,    
    sd.gender, 
    null as iep_date,
    null as re_evl_date,
    'ACTIVE' as status,
    now() as created_at,
    now() as updated_at,
    null as parent1_phone_number,
    null as parent1_email,
    null as parent2_phone_number,
    null as parent2_email,
    distadmin.user_id as created_by,
    null as updated_by,
    null as physician_name,
    null as npi_number,
    null as unique_profile_id
from cte_students as sd
inner join portal.districts as d on sd.school_district = d.name    
inner join portal.schools as scl on sd.building = scl.name
inner join portal.grade as grd on sd.grade = grd.grade
inner join portal.primary_language as l on sd.language = l.name
inner join (
	select ru.user_id, ud.district_id, row_number() over(partition by ud.district_id order by ud.updated_at desc) as rownum  
    from portal.roles_to_users as ru
    inner join portal.user_districts as ud on ru.user_id = ud.user_id
	where ru.role_id='school_admin'	
) as distadmin on d.id = distadmin.district_id and distadmin.rownum = 1
where sd.rownum = 1

