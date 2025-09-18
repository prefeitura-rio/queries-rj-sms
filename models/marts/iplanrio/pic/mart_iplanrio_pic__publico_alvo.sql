{{ config(
    alias = "publico_alvo",
    materialized = "table"
) }}

with

    -- ------------------------------------------------------------
    -- Gestantes em Andamento
    -- ------------------------------------------------------------
    gestacoes_em_andamento as (
        select
            cpf,
            data_diagnostico as data_referencia,
            'Gestacao' as tipo_publico
        from {{ ref('mart_linhas_cuidado__gestacoes') }}
        where tipo_transicao = 'Em Andamento'
    ),

    -- ------------------------------------------------------------
    -- Gestantes em Puerpério
    -- ------------------------------------------------------------
    gestacoes_encerradas_em_puerperio as (
        select
            cpf,
            data_diagnostico_seguinte  as data_referencia,
            'Puerperio' as tipo_publico
        from {{ ref('mart_linhas_cuidado__gestacoes') }}
        where tipo_transicao = 'Encerramento Comprovado' and date_diff(current_date(), data_diagnostico_seguinte, day) <= 42
    ),

    -- ------------------------------------------------------------
    -- Criancas
    -- ------------------------------------------------------------
    criancas as (
        SELECT
            cpf,
            data_nascimento,
            'Infancia' as tipo_publico
        FROM {{ ref("raw_prontuario_vitacare__paciente") }}
        WHERE data_nascimento > DATE_SUB(CURRENT_DATE(), INTERVAL 6 YEAR) and cpf <> 'NAO TEM'
        qualify row_number() over (partition by cpf order by source_updated_at desc) = 1
    ),

    -- ------------------------------------------------------------
    -- Junção dos casos
    -- ------------------------------------------------------------
    juncao_casos as (
        select * from gestacoes_em_andamento
        union all
        select * from gestacoes_encerradas_em_puerperio
        union all
        select * from criancas
    )
select 
    *,
    struct(
        current_timestamp() as ultima_atualizacao
    ) as metadados
from juncao_casos