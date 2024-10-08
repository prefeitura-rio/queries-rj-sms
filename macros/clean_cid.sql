
{% macro clean_cid(text) %}
CASE 
WHEN regexp_contains(lower({{text}}),'gravidez.*') THEN {{text}}
ELSE
    regexp_replace(
        regexp_replace(
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(
                            lower({{text}}),
                            r'(em circunstâncias não especificadas)|(e a causas não especificadas)|(não classificad[o|a]s em outra parte)|([(sem)|(com)] confirmação bacteriológica ou histológica)|((, ){0,1}e [à|a|o]s não especificad[a|o]s)|(outras formas e as não especificadas da )|(de localização não especificada)|(^alguns)|(algumas)|(emissão de prescrição de repetição)|(, nível não especificado)',
                            ''
                        ),
                        '(^outr[a|o]s{0,1}( (tipos|formas) de ){0,1})|(não especificadas)',
                        ''
                    ),
                    r'.*[E|e]xames* .*',
                    'Exame'
                ),
                r'(^ +)|(,{0,1} {0,1}$)|[( [][ a-z-à-ü-0-9]+[])]|(,,)',
                ''
            ),
            ' {2,}',
            ' '
        ),
        ' {1,}, {1,}',
        ', '
    )
END

{% endmacro %}