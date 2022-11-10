{% macro insert_students() %}

INSERT INTO `test`.`students`
(
`student_id`,
`first_name`,
`last_name`,
`district_id`,
`school_id`,
`icdCode_id`,
`grade_id`,
`primaryLanguage_id`,
`contact_phone_number`,
`contact_email`,
`birth_date`,
`gender`,
`iep_date`,
`re_evl_date`,
`status`,
`created_at`,
`updated_at`)
select 
    student_id,
    first_name,
    last_name,
    district_id,
    school_id,
    icdCode_id,
    grade_id,
    primaryLanguage_id,
    parent1_phone_number,
    parent1_email,
    birth_date,
    gender,
    iep_date,
    re_evl_date,
    status,
    created_at,
    updated_at
from testdataimport.stg_students
where `updated_at` >= (select max(`created_at`) from `testdataimport`.`transform_log`)

{% endmacro %}