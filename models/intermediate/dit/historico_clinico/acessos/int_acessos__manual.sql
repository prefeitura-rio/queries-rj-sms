{{
    config(
        schema="intermediario_historico_clinico",
        alias="acessos_manual",
        materialized="table",
    )
}}

with
    usuarios_permitidos_sheets as (
        select
            *
        from {{ ref('raw_sheets__usuarios_permitidos_hci') }}
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
            funcao as funcao_detalhada,            
            CASE
                WHEN {{ remove_accents_upper('funcao') }} like '%ENFERM%' THEN 'ENFERMEIROS'
                WHEN {{ remove_accents_upper('funcao') }} like '%COORDENA%' THEN 'MEDICOS'
                WHEN {{ remove_accents_upper('funcao') }} like '%MEDICO%' THEN 'MEDICOS'
                WHEN {{ remove_accents_upper('funcao') }} like '%CONVENIO%' THEN 'MEDICOS'
                ELSE null
            END as funcao_grupo,
        from usuarios_permitidos_sheets
    )

select
    cpf,
    nome_completo,
    unidade_nome,
    unidade_tipo,
    unidade_cnes,
    funcao_detalhada,
    funcao_grupo
from categorizados
where {{ validate_cpf('cpf') }}
