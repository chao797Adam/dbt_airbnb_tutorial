{% macro clean_string(column_name) %}
    -- 先 trim 去掉前后空格，再转成大写
    UPPER(TRIM({{ column_name }}))
{% endmacro %}