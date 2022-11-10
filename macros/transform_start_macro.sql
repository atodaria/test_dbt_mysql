{% macro insert_transform_log() %}
insert into testdataimport.transform_log(created_at)
select now() as created_at
{% endmacro %}