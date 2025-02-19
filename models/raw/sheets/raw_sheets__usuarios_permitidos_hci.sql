{{
    config(
        schema="brutos_sheets",
        alias="usuarios_permitidos_hci",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}

with
    source as (
        select *
        from {{ source("brutos_sheets_staging", "usuarios_permitidos_hci") }}
    ),

    tratados as (
        select
            safe_cast(cpf as int64) as cpf_particao,
            lpad(safe_cast(cpf as string), 11, "0") as cpf,
            trim(nome_unidade) as unidade_nome,
            funcao as funcao_detalhada,
        from source
    ),

    categorizados as (
        select 
            *,
            CASE
                WHEN unidade_nome = 'SUBHUE' THEN '0932280' -- Esse caso não devia existir, mas temporariamente botei UPA
                WHEN unidade_nome = 'UPA Cidade de Deus' THEN '6575900'
                WHEN unidade_nome = 'UPA Del Castilho' THEN '0932280'
                WHEN unidade_nome = 'UPA Paciência' THEN '6938124'
                WHEN unidade_nome = 'UPA Rocinha' THEN '6507409'
                WHEN unidade_nome = 'UPA Senador Camará' THEN '6742831'
                WHEN unidade_nome = 'UPA Rocha Miranda' THEN '7110162'
                WHEN unidade_nome = 'CER Barra' THEN '6716938'
                WHEN unidade_nome = 'CER Leblon' THEN '6716849'
                ELSE null
            END as unidade_cnes,
            CASE
                WHEN unidade_nome = 'SUBHUE' THEN 'UPA' -- Esse caso não devia existir, mas temporariamente botei UPA
                WHEN unidade_nome like 'UPA%' THEN 'UPA'
                WHEN unidade_nome like 'CER%' THEN 'CER'
                ELSE null
            END as unidade_tipo,            
            CASE
                WHEN {{ remove_accents_upper('funcao_detalhada') }} like '%ENFERM%' THEN 'ENFERMEIROS'
                WHEN {{ remove_accents_upper('funcao_detalhada') }} like '%COORDENA%' THEN 'MEDICOS'
                WHEN {{ remove_accents_upper('funcao_detalhada') }} like '%MEDICO%' THEN 'MEDICOS'
                WHEN {{ remove_accents_upper('funcao_detalhada') }} like '%CONVENIO%' THEN 'MEDICOS'
                ELSE null
            END as funcao_grupo,
        from tratados
    ),

    distintos as (
        select distinct *
        from categorizados
    )

select
    cpf_particao,
    cpf,
    unidade_nome,
    unidade_tipo,
    unidade_cnes,
    funcao_detalhada,
    funcao_grupo
from distintos
where {{ validate_cpf('cpf') }}
