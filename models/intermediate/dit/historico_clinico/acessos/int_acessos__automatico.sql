{{
    config(
        schema="intermediario_historico_clinico",
        alias="acessos_automatico",
        materialized="table",
    )
}}

with
    profissionais_cnes as (
        select
            id_profissional_sus,
            nome,
            cns,
            cpf
        from {{ ref("raw_cnes_gdb__profissional") }}
    ),
    unidades_de_saude as (
        select
            id_cnes,
            id_unidade,
            area_programatica,
            tipo_sms_simplificado,
            nome_limpo as unidade_nome
        from {{ ref("dim_estabelecimento") }}
    ),
    cbo_datasus as (
        select * from {{ ref("raw_datasus__cbo") }}
    ),
    vinculos_profissionais_cnes as (
        select
            -- cartao_nacional_saude as cns,
            id_profissional_sus,
            id_cnes,
            unidades_de_saude.area_programatica,
            unidades_de_saude.tipo_sms_simplificado,
            unidades_de_saude.unidade_nome,
            cbo_datasus.descricao as cbo_nome,
            case 
                when regexp_contains(lower(cbo_datasus.descricao),'^medic')
                    then 'MÉDICOS'
                when regexp_contains(lower(cbo_datasus.descricao),'^cirurgiao[ |-|]dentista')
                    then 'DENTISTAS'
                when regexp_contains(lower(cbo_datasus.descricao),'psic')
                    then 'PSICÓLOGOS'  
                when regexp_contains(lower(cbo_datasus.descricao),'fisioterap')
                    then 'FISIOTERAPEUTAS'
                when regexp_contains(lower(cbo_datasus.descricao),'nutri[ç|c]')
                    then 'NUTRICIONISTAS'
                when regexp_contains(lower(cbo_datasus.descricao),'fono')
                    then 'FONOAUDIÓLOGOS'   
                when regexp_contains(lower(cbo_datasus.descricao),'farm')
                    then 'FARMACÊUTICOS'  
                when (
                        (regexp_contains(lower(cbo_datasus.descricao),'enferm')) and 
                        (lower(cbo_datasus.descricao) !='socorrista (exceto medicos e enfermeiros)') and
                        (not regexp_contains(lower(cbo_datasus.descricao),'tecnico'))
                    )
                    then 'ENFERMEIROS' 
                when cbo_datasus.descricao in ('Dirigente do servico publico municipal',
                'Diretor de servicos de saude','Gerente de servicos de saude')
                    then 'DIRETORES DE SAUDE' 
                else
                    'OUTROS PROFISSIONAIS'
            end as cbo_agrupador,
            data_ultima_atualizacao,
        from {{ ref("raw_cnes_gdb__vinculo") }} as gdb_profissional
        left join cbo_datasus using (id_cbo)
        inner join unidades_de_saude using (id_unidade)
    ),

    -- -----------------------------------------
    -- Enriquecimento de Dados dos Funcionários
    -- -----------------------------------------
    funcionarios_ativos_enriquecido as (
        select distinct
            cpf,
            nome as nome_completo,
            unidade_nome,
            tipo_sms_simplificado as unidade_tipo,
            id_cnes as unidade_cnes,
            area_programatica as unidade_ap,
            cbo_nome as funcao_detalhada,
            {{ remove_accents_upper('cbo_agrupador') }} as funcao_grupo,
            data_ultima_atualizacao
        from vinculos_profissionais_cnes
            left join profissionais_cnes using (id_profissional_sus)
    ),
    -- -----------------------------------------
    -- Filtrando funcionários com acesso autorizado
    -- -----------------------------------------
    funcionarios_ativos_enriquecido_autorizados as (    
        select
            cpf,
            upper(nome_completo) as nome_completo,
            unidade_nome,
            unidade_tipo,
            unidade_cnes,
            unidade_ap,
            funcao_detalhada,
            funcao_grupo
        from funcionarios_ativos_enriquecido
        where 
            -- Critérios de Lançamento
            funcao_grupo in (
                'MEDICOS',
                'ENFERMEIROS',
                'DIRETORES DE SAUDE'
            )
    ),
    -- -----------------------------------------
    -- Agrupando vinculos de profissionais
    -- -----------------------------------------
    funcionario_vinculos as (
        select 
            cpf,
            nome_completo,
            array_agg(
                struct(
                    unidade_nome,
                    unidade_tipo,
                    unidade_cnes,
                    unidade_ap,
                    funcao_detalhada,
                    funcao_grupo,
                    case
                        when unidade_tipo in ('UPA','HOSPITAL', 'CER', 'CCO','MATERNIDADE')
                        then 'full_permission'
                        when unidade_tipo in ('CMS','POLICLINICA','CF','CMR','CSE')
                        then 'only_from_same_cnes'
                        when (unidade_tipo in ('CGS'))
                        then 'only_from_same_ap'
                        ELSE null
                    end as nivel_acesso
                )
            ) as vinculos 
        from funcionarios_ativos_enriquecido_autorizados
        group by 1,2
    )

    select 
        cpf,
        nome_completo,
        {{ dedup_array_of_struct('vinculos')}} as vinculos
    from funcionario_vinculos