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
            * except(telefone, email, unidade, descricao, observacoes),
            unidade as unidade_cnes,
            funcao as funcao_detalhada,
            case
                when {{ remove_accents_upper('funcao') }} like '%ENFERM%' then 'ENFERMEIROS'
                when {{ remove_accents_upper('funcao') }} like '%MEDICO%' then 'MEDICOS'
                when {{ remove_accents_upper('funcao') }} like '%CONVENIO%' then 'MEDICOS'
                when {{ remove_accents_upper('funcao') }} like '%CIENTI%' then 'DESENVOLVEDOR'
                when {{ remove_accents_upper('funcao') }} like '%DESENV%' then 'DESENVOLVEDOR'
                when {{ remove_accents_upper('funcao') }} like '%ANALISTA DE VIGIL%' then 'ANALISTA DE VIGILANCIA'
                else 'OUTROS'
            end as funcao_grupo,
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
            nivel_de_acesso as nivel_acesso,
            granularidade_de_acesso as granularidade_acesso
        from categorizados
        left join unidades_de_saude
            on categorizados.unidade_cnes = unidades_de_saude.cnes
    ),
    agrupa_vinculos as (
        select
            cpf,
            nome_completo,
            array_agg(
                struct(
                    unidade_nome,
                    unidade_tipo,
                    unidade_cnes,
                    unidade_ap,
                    false as eh_equipe_consultorio_rua,
                    funcao_detalhada,
                    funcao_grupo,
                    nivel_acesso,
                    granularidade_acesso
                )
            ) as vinculos
        from categorizados_enriquecidos
        group by 1,2
    )
select
    cpf,
    nome_completo,
    vinculos
from agrupa_vinculos
