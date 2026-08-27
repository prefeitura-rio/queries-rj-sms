{{
    config(
        alias='numeros_principais',
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['data_registro'],
    )
}}

WITH

    atendimentos as (
        SELECT
            COUNT(*) as qtd_atendimentos
        FROM {{ ref("mart_historico_clinico__episodio") }}
    ),

    cadastros as (
        SELECT
            COUNT(*) as qtd_cadastros
        FROM {{ ref("mart_historico_clinico__paciente") }}
    ),

    exames_imagem as (
        SELECT
            COUNT(*) as qtd_exames_imagem
        FROM {{ ref("mart_historico_clinico__exame_imagem") }}
    ),

    exames_laboratoriais as (
        SELECT
            COUNT(*) as qtd_exames_laboratoriais
        FROM {{ ref("mart_historico_clinico__exame_laboratorial") }}
    ),

    vacinacoes as (
        SELECT
            COUNT(*) as qtd_vacinacoes
        FROM {{ ref("mart_historico_clinico__vacinacao") }}
    ),

    juncao_numeros_principais as (
        -- Dados fictícios para ambiente de desenvolvimento
        {%if target.name in ['dev', 'ci']%}
            SELECT 
                data_registro,
                CAST(RAND() * 10000 AS INT64) as qtd_atendimentos,
                CAST(RAND() * 5000 AS INT64) as qtd_cadastros,
                CAST(RAND() * 2000 AS INT64) as qtd_exames_imagem,
                CAST(RAND() * 3000 AS INT64) as qtd_exames_laboratoriais,
                CAST(RAND() * 1500 AS INT64) as qtd_vacinacoes
            FROM UNNEST(GENERATE_DATE_ARRAY('2026-07-01', '2026-08-10')) as data_registro
        {% else %}
        SELECT 
            current_date('America/Sao_Paulo') as data_registro,
            atendimentos.qtd_atendimentos,
            cadastros.qtd_cadastros,
            exames_imagem.qtd_exames_imagem,
            exames_laboratoriais.qtd_exames_laboratoriais,
            vacinacoes.qtd_vacinacoes
        FROM atendimentos, cadastros, exames_imagem, exames_laboratoriais, vacinacoes

        {% endif %}
    )

select * 
from juncao_numeros_principais