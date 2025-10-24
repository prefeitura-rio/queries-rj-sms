{{
    config(
        alias='numeros_principais',
        materialized='table',
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
        SELECT 
            atendimentos.qtd_atendimentos,
            cadastros.qtd_cadastros,
            exames_imagem.qtd_exames_imagem,
            exames_laboratoriais.qtd_exames_laboratoriais,
            vacinacoes.qtd_vacinacoes
        FROM atendimentos, cadastros, exames_imagem, exames_laboratoriais, vacinacoes
    )
select * 
from juncao_numeros_principais