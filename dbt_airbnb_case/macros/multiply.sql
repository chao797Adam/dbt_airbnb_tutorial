{% macro multiply_and_round(col1, col2, precision=2) %}
    ROUND(CAST({{ col1 }} AS DOUBLE) * CAST({{ col2 }} AS DOUBLE), {{ precision }})
{% endmacro %}