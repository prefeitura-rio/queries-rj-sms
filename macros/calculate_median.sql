{% macro calculate_median_sql(array_field) %}
    (
        if(
            mod(array_length({{ array_field }}), 2) = 1,
            {{ array_field }} [offset(div(array_length({{ array_field }}), 2))],
            (
                {{ array_field }} [offset(div(array_length({{ array_field }}), 2) - 1)]
                + {{ array_field }} [offset(div(array_length({{ array_field }}), 2))]
            )
            / 2
        )
    )
{% endmacro %}
