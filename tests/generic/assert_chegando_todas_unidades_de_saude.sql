{% test assert_chegando_todas_unidades_de_saude(
    model, column_name, prontuario, filter
) %}
        with
            unidades_saude_presentes as (
                select distinct {{ column_name }} from {{ model }}
            ),
            unidades_saude_relacao_completa as (
                select id_cnes, area_programatica, nome_limpo, prontuario_versao,
                from {{ ref("dim_estabelecimento") }}
                where
                    prontuario_tem = "sim" and prontuario_versao = '{{ prontuario }}'
                    {% if filter is defined %} and {{ filter }} {% endif %}
            ),
            unidade_saude_faltantes as (
                select relacao_completa.*
                from unidades_saude_relacao_completa as relacao_completa
                left join
                    unidades_saude_presentes as presentes
                    on presentes.{{ column_name }} = relacao_completa.id_cnes
                where presentes.{{ column_name }} is null
            )
        select *
        from unidade_saude_faltantes where FORMAT_DATE('%A', CURRENT_DATE()) != 'sunday' or prontuario_versao != 'vitacare' -- filtro aplicado para evitar a execução, apenas nas unidades de atenção primaria que funcionam aos domingos --
{% endtest %}
