{% macro convert_bool_to_int(column_name, table_alias) %}
case when {{ table_alias }}.{{ column_name }} = True then 1 else 0 end as {{ column_name}}
{% endmacro %}