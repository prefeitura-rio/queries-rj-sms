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
        from {{ ref("int_gdb_cnes__profissional") }}
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
        select *
        from {{ ref("raw_datasus__cbo") }}
    ),
    vinculos_dedup as (
        select *
        from {{ ref("int_gdb_cnes__vinculo") }}
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
                when regexp_contains(lower(cbo_datasus.descricao), '^medic')
                    then 'MÉDICOS'
                when regexp_contains(lower(cbo_datasus.descricao), r'^cirurgiao[\s\-]*dentista')
                    then 'DENTISTAS'
                when regexp_contains(lower(cbo_datasus.descricao), 'psic')
                    then 'PSICÓLOGOS'
                when regexp_contains(lower(cbo_datasus.descricao), 'fisioterap')
                    then 'FISIOTERAPEUTAS'
                when regexp_contains(lower(cbo_datasus.descricao), 'nutri[çc]')
                    then 'NUTRICIONISTAS'
                when regexp_contains(lower(cbo_datasus.descricao), 'fonoaudio')
                    then 'FONOAUDIÓLOGOS'
                when regexp_contains(lower(cbo_datasus.descricao), 'farm')
                    then 'FARMACÊUTICOS'
                when (
                        (regexp_contains(lower(cbo_datasus.descricao),'enferm')) and 
                        (lower(cbo_datasus.descricao) !='socorrista (exceto medicos e enfermeiros)') and
                        (not regexp_contains(lower(cbo_datasus.descricao),'tecnico')) and
                        (not regexp_contains(lower(cbo_datasus.descricao),'auxiliar')) and
                        (not regexp_contains(lower(cbo_datasus.descricao),'atendente')) 
                    )
                    then 'ENFERMEIROS'
                when lower(cbo_datasus.descricao) = 'dirigente do servico publico municipal'
                    then 'DIRIGENTES DE SAUDE'
                when lower(cbo_datasus.descricao) in (
                    'diretor de servicos de saude',
                    'gerente de servicos de saude'
                )
                    then 'DIRETORES DE SAUDE'
                -- Ago/2026 - Sanitaristas:
                -- Médicos e enfermeiros sanitaristas terão sido filtrados pelas condicionais
                -- anteriores, mas não custa deixar aqui por completude
                when id_cbo in (
                    '223560',  -- Enfermeiro sanitarista
                    '223156',  -- Medico sanitarista
                    '225139',  -- Medico sanitarista
                    '131225',  -- Sanitarista
                    '1312C1'   -- Sanitarista
                )
                    then 'SANITARISTAS'
                -- Ago/2025 - Personas de acesso:
                -- * Assistentes administrativos do Complexo Regulador (CNES 7106513/3304557106513)
                when id_cnes = '7106513' and id_cbo = '411010'
                    then 'ADMINISTRATIVO'
                else
                    'OUTROS PROFISSIONAIS'
            end as cbo_agrupador,
            data_ultima_atualizacao,
        from vinculos_dedup
        left join cbo_datasus using (id_cbo)
        inner join unidades_de_saude using (id_unidade)
    ),
    -- -----------------------------------------
    -- Lista profissionais alocados em consultórios de rua
    -- -----------------------------------------
    profissionais_consultorio_rua as (
        select
            prof.id_profissional_sus,
            eq.id_equipe_tipo,
            eq.equipe_descricao
        from {{ ref("int_gdb_cnes__equipe_profissionais") }} as prof
        left join {{ ref("int_gdb_cnes__equipe") }} as eq
            using (equipe_sequencial)
        where eq.id_equipe_tipo = '73'
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
            case 
                when id_equipe_tipo = '73' then true
                else false
            end as eh_equipe_consultorio_rua,
            cbo_nome as funcao_detalhada,
            {{ remove_accents_upper('cbo_agrupador') }} as funcao_grupo,
            data_ultima_atualizacao
        from vinculos_profissionais_cnes
        left join profissionais_cnes using (id_profissional_sus)
        left join profissionais_consultorio_rua using (id_profissional_sus)
    ),

    -- -----------------------------------------
    -- Filtrando funcionários com acesso autorizado
    -- -----------------------------------------
    ergon_ativos as (
        select distinct
            cpf
        from {{ ref("raw_ergon__funcionarios_sms")}},
            unnest(vinculos) as vinculo
        where vinculo.status_ativo = true
    ),
    funcionarios_ativos_enriquecido_autorizados as (
        select
            cpf,
            upper(nome_completo) as nome_completo,
            unidade_nome,
            unidade_tipo,
            unidade_cnes,
            unidade_ap,
            eh_equipe_consultorio_rua,
            funcao_detalhada,
            funcao_grupo
        from funcionarios_ativos_enriquecido
        where 
            -- Critérios de Lançamento
            funcao_grupo in (
                'MEDICOS',
                'ENFERMEIROS',
                'SANITARISTAS',
                'DENTISTAS',
                'DIRETORES DE SAUDE',
                'DIRIGENTES DE SAUDE',
                'ADMINISTRATIVO'
            )
            and cpf in (
                select *
                from ergon_ativos
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
                    eh_equipe_consultorio_rua,
                    funcao_detalhada,
                    funcao_grupo,
                    case
                        when eh_equipe_consultorio_rua is true
                            then 'full_permission'
                        when funcao_grupo = 'SANITARISTAS'
                            then 'full_permission'
                        when unidade_tipo in (
                            'UPA', 'HOSPITAL', 'CER', 'CE',
                            'MATERNIDADE', 'CENTRAL DE REGULACAO', 'CASS',
                            'PRISIONAL'
                        )
                            then 'full_permission'
                        when funcao_grupo = 'DIRIGENTES DE SAUDE'
                            then 'full_permission'
                        when funcao_grupo = 'DIRETORES DE SAUDE'
                            then 'only_from_same_ap'
                        when unidade_tipo in ('CGS','CAPS') and funcao_grupo in ('MEDICOS','DENTISTAS')
                            then 'full_permission'
                        when unidade_tipo in ('CGS','CAPS') and funcao_grupo not in ('MEDICOS','DENTISTAS')
                            then 'only_from_same_ap'
                        when unidade_tipo in ('CMS','POLICLINICA','CF','CMR','CSE') and funcao_grupo in ('MEDICOS','DENTISTAS')
                            then 'full_permission'
                        when unidade_tipo in ('CMS','POLICLINICA','CF','CMR','CSE') and funcao_grupo not in ('MEDICOS','DENTISTAS')
                            then 'only_from_same_cnes'
                        else null
                    end as nivel_acesso,
                    case
                        when (unidade_cnes = '7106513' and funcao_grupo = 'ADMINISTRATIVO')
                            then 'only_header'
                        else 'full_permission'
                    end as granularidade_acesso
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
