{% macro calculate_lev(input1,input2) %}
    (
        edit_distance({{input1}}, {{input2}})/if(least(char_length({{input1}}), char_length({{input2}}))=0,
        1,
        least(char_length({{input1}}), char_length({{input2}})))
    )
{% endmacro %}
