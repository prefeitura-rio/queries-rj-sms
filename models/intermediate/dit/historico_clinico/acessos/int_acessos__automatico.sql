{{
    config(
        schema="intermediario_historico_clinico",
        alias="acessos_automatico",
        materialized="table",
    )
}}

with
    funcionarios_ativos_ergon as (
        select
            distinct cpf
        from {{ ref("raw_ergon_funcionarios") }}, unnest(dados) as funcionario_dado
        where status_ativo = true
    ),
    profissionais_cnes as (
        select
            cpf,
            cns,
            nome
        from {{ ref("dim_profissional_saude") }}
    ),
    unidades_de_saude as (
        select
            id_cnes as cnes,
            tipo_sms_simplificado,
            nome_limpo as unidade_nome
        from {{ ref("dim_estabelecimento") }}
    ),
    vinculos_profissionais_cnes as (
        select
            profissional_cns as cns,
            id_cnes as cnes,
            cbo_nome,
            cbo_agrupador,
            data_ultima_atualizacao
        from {{ ref("dim_vinculo_profissional_saude_estabelecimento") }}
    ),

    -- -----------------------------------------
    -- Enriquecimento de Dados dos Funcionários
    -- -----------------------------------------
    funcionarios_ativos_enriquecido as (
        select
            cpf,
            nome as nome_completo,
            unidade_nome,
            tipo_sms_simplificado as unidade_tipo,
            cnes as unidade_cnes,
            cbo_nome as funcao_detalhada,
            {{ remove_accents_upper('cbo_agrupador') }} as funcao_grupo,
            data_ultima_atualizacao
        from funcionarios_ativos_ergon
            left join profissionais_cnes using (cpf)
            left join vinculos_profissionais_cnes using (cns)
            left join unidades_de_saude using (cnes)
    ),
    -- -----------------------------------------
    -- Pegando Vinculo mais recente
    -- -----------------------------------------
    funcionarios_ativos_enriquecido_ranked as (
        select
            *,
            row_number() over (partition by cpf order by data_ultima_atualizacao desc) as rn
        from funcionarios_ativos_enriquecido
    ),
    funcionarios_ativos_enriquecido_mais_recente as (
        select
            * except(rn, data_ultima_atualizacao)
        from funcionarios_ativos_enriquecido_ranked
        where rn = 1
    )

select
    cpf,
    nome_completo,
    unidade_nome,
    unidade_tipo,
    unidade_cnes,
    funcao_detalhada,
    funcao_grupo
from funcionarios_ativos_enriquecido_mais_recente