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
    profissionais_cnes_unnest as (
        select
            cpf,
            nome,
            cns
        from {{ ref("dim_profissional_saude") }}
        union all 
        select
            cpf,
            dados.nome as nome,
            c as cns
        from {{ ref("mart_historico_clinico__paciente") }}, unnest(cns) as c
    ),
    profissionais_cnes as (
        select distinct 
            cpf,
            nome,
            cns
        from profissionais_cnes_unnest
    ),
    unidades_de_saude as (
        select
            id_cnes as cnes,
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
            cartao_nacional_saude as cns,
            id_estabelecimento_cnes as cnes,
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
                else
                    'OUTROS PROFISSIONAIS'
            end as cbo_agrupador,
            concat(ano, '-', lpad(cast(mes as string), 2, '0')) data_ultima_atualizacao,
        from {{ ref("raw_cnes_ftp__profissional") }} as ftp_profissional
        left join cbo_datasus on ftp_profissional.cbo_2002=cbo_datasus.id_cbo
        inner join unidades_de_saude on ftp_profissional.id_estabelecimento_cnes = unidades_de_saude.cnes
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
            cnes as unidade_cnes,
            area_programatica as unidade_ap,
            cbo_nome as funcao_detalhada,
            {{ remove_accents_upper('cbo_agrupador') }} as funcao_grupo,
            data_ultima_atualizacao
        from funcionarios_ativos_ergon
            left join profissionais_cnes using (cpf)
            left join vinculos_profissionais_cnes using (cns)
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
    upper(nome_completo) as nome_completo,
    unidade_nome,
    unidade_tipo,
    unidade_cnes,
    unidade_ap,
    funcao_detalhada,
    funcao_grupo,
    safe_cast(null as string) as nivel_de_acesso
from funcionarios_ativos_enriquecido_mais_recente
where 
    -- Critérios de Lançamento
    (
        unidade_ap in ('51') or 
        unidade_cnes in (
            '6507409',
            '6575900',
            '6507409',
            '7110162',
            '0932280',
            '6716849',
            '6938124',
            '6512925',
            '6742831',
            '6503772',
            '2280183'
        )
    )
    and funcao_grupo in (
        'MEDICOS',
        'ENFERMEIROS'
    )