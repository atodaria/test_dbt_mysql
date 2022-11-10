{% macro update_transform_log() %}
with maxid_cte as (select max(id) as maxid from testdataimport.transform_log)
update testdataimport.transform_log
set completed_at=now()
where id=(select maxid from maxid_cte)
{% endmacro %}