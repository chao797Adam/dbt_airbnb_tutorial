{% macro trimmer(column_name) %} {{ column_name | trim | upper }} {% endmacro %}
