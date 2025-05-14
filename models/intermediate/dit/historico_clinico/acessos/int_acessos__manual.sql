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
            unidade as unidade_cnes,
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
    struct(
    nivel_de_acesso as nivel_acesso_descricao,
    case 
        when nivel_de_acesso = 'full_permission' then 4
        when nivel_de_acesso = 'full_permition' then 4
        when nivel_de_acesso = 'only_from_same_ap' then 3
        when nivel_de_acesso = 'only_from_same_cnes' then 2
        when nivel_de_acesso = 'only_from_same_cpf' then 1
        else null
    end as nivel_acesso_rank
    ) as acesso
from categorizados_enriquecidos
where {{ validate_cpf('cpf') }}