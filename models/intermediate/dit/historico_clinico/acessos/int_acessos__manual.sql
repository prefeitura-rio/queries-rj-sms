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
            * except(unidade),
            CASE
                WHEN unidade IN ('DIT','SUBHUE','SUBPAV','SUBGERAL','SUBGESTÃO','SMS') THEN '0932280'
                WHEN unidade = 'UPA Cidade de Deus' THEN '6575900'
                WHEN unidade = 'UPA Del Castilho' THEN '0932280'
                WHEN unidade = 'UPA Paciência' THEN '6938124'
                WHEN unidade = 'UPA Rocinha' THEN '6507409'
                WHEN unidade = 'UPA Senador Camará' THEN '6742831'
                WHEN unidade = 'UPA Rocha Miranda' THEN '7110162'
                WHEN unidade = 'CER Barra' THEN '6716938'
                WHEN unidade = 'CER Leblon' THEN '6716849'
                WHEN safe_cast(unidade as int64) is not null THEN unidade
                ELSE null
            END as unidade_cnes,
            funcao as funcao_detalhada,            
            CASE
                WHEN {{ remove_accents_upper('funcao') }} like '%ENFERM%' THEN 'ENFERMEIROS'
                WHEN {{ remove_accents_upper('funcao') }} like '%MEDICO%' THEN 'MEDICOS'
                WHEN {{ remove_accents_upper('funcao') }} like '%CONVENIO%' THEN 'MEDICOS'
                WHEN {{ remove_accents_upper('funcao') }} like '%CIENTI%' THEN 'DESENVOLVEDOR'
                WHEN {{ remove_accents_upper('funcao') }} like '%DESENV%' THEN 'DESENVOLVEDOR'
                WHEN {{ remove_accents_upper('funcao') }} like '%ANALISTA DE VIGIL%' THEN 'ANALISTA DE VIGILANCIA'
                ELSE 'OUTROS'
            END as funcao_grupo,
        from usuarios_permitidos_sheets
    ),

    -- -----------------------------------------
    -- Enriquecimento
    -- -----------------------------------------
    unidades_de_saude as (
        select
            id_cnes as cnes,
            area_programatica,
            tipo_sms_simplificado,
            nome_limpo as unidade_nome
        from {{ ref("dim_estabelecimento") }}
    ),

    categorizados_enriquecidos as (
        select
            cpf,
            upper(nome_completo) as nome_completo,
            unidade_cnes,
            unidades_de_saude.area_programatica as unidade_ap,
            unidades_de_saude.tipo_sms_simplificado as unidade_tipo,
            unidades_de_saude.unidade_nome,
            funcao_detalhada,
            funcao_grupo,
            nivel_de_acesso
        from categorizados
        left join unidades_de_saude
            on categorizados.unidade_cnes = unidades_de_saude.cnes
    )

select
    cpf,
    nome_completo,
    unidade_nome,
    unidade_tipo,
    unidade_cnes,
    unidade_ap,
    funcao_detalhada,
    funcao_grupo,
    nivel_de_acesso
from categorizados_enriquecidos
where {{ validate_cpf('cpf') }}