{{
    config(
        schema="intermediario_historico_clinico",
        alias="vinculo_equipe_familia",
        materialized="table",
        cluster_by=['cpf_profissional']
    )
}}

with
    -- Seleciona os atendimentos com CNES e data válidos e prepara os campos
    -- usados para identificar paciente, unidade e profissional
    atendimentos_elegiveis as (

        select
            nullif(trim(cast(id_prontuario_global as string)), '') as id_paciente_atendimento,
            {{ process_null('cpf') }} as cpf_paciente,
            {{ process_null('cnes_unidade') }} as id_cnes,
            datahora_inicio as data_atendimento,
            left(cast(cbo_profissional as string), 3) as cbo_3,
            left(cast(cbo_profissional as string), 4) as cbo_4,
            lower(cbo_descricao_profissional) as cbo_descricao
        from {{ ref('raw_prontuario_vitacare__atendimento') }}
        where {{ process_null('cnes_unidade') }} is not null
          and datahora_inicio is not null

    ),

    atendimentos_com_chave_paciente as (

        select
            coalesce(
                id_paciente_atendimento,
                cpf_paciente
            ) as chave_paciente,
            id_cnes,
            data_atendimento,
            cbo_3,
            cbo_4,
            cbo_descricao
        from atendimentos_elegiveis
        where coalesce(
                id_paciente_atendimento,
                cpf_paciente
              ) is not null

    ),

    -- Ordena os atendimentos por paciente ao longo do tempo e também por paciente+unidade para identificar blocos contínuos na mesma unidade
    atendimentos_ordenados as (

        select
            chave_paciente,
            id_cnes,
            data_atendimento,
            cbo_3,
            cbo_4,
            cbo_descricao,
            row_number() over (
                partition by chave_paciente
                order by data_atendimento
            ) as rn_geral,
            row_number() over (
                partition by chave_paciente, id_cnes
                order by data_atendimento
            ) as rn_unidade
        from atendimentos_com_chave_paciente

    ),

    -- Agrupa os atendimentos em sequências contínuas na mesma unidade e conta, dentro de cada bloco, quantos atendimentos foram com médico/enfermeiro
    sequencias_consultas as (

        select
            chave_paciente,
            id_cnes,
            rn_geral - rn_unidade as grupo_sequencia,
            countif(
                cbo_descricao like '%medico%'
                or cbo_descricao like '%médico%'
                or cbo_descricao like '%enfer%'
            ) as qtd_consultas_seguidas
        from atendimentos_ordenados
        group by 1, 2, 3

    ),

    -- Mantém apenas os pacientes temporários que tiveram mais de 5 consultas com médico/enfermeiro em uma sequência contínua na mesma unidade
    pacientes_temporarios_elegiveis as (

        select distinct
            chave_paciente,
            id_cnes
        from sequencias_consultas
        where qtd_consultas_seguidas > 5

    ),

    paciente_base as (

        select
            p.id as id_paciente,
            {{ process_null('p.id_cnes') }} as id_cnes,
            {{ process_null('p.cpf') }} as cpf_paciente,
            {{ process_null('p.cns') }} as cns_paciente,
            p.nome as nome_paciente,
            p.mae_nome,
            p.data_nascimento,
            p.situacao,
            p.cadastro_permanente_indicador,
            p.equipe_familia_indicador,
            {{ process_null('p.id_ine') }} as id_ine,
            p.data_atualizacao_vinculo_equipe,
            p.data_ultima_atualizacao_cadastral,
            p.source_updated_at,
            p.updated_at_rank,
            coalesce(
                nullif(trim(cast(p.id as string)), ''),
                {{ process_null('p.cpf') }},
                {{ process_null('p.cns') }}
            ) as chave_paciente
        from {{ ref('raw_prontuario_vitacare__paciente') }} p
        where {{ process_null('p.id_ine') }} is not null

    ),

    -- Mantém os pacientes elegíveis: cadastro permanente entra direto; temporários só entram se forem elegíveis pela regra das consultas
    pacientes_elegiveis as (

        select
            p.*
        from paciente_base p
        left join pacientes_temporarios_elegiveis t
            on p.chave_paciente = t.chave_paciente
           and p.id_cnes = t.id_cnes
        where p.cadastro_permanente_indicador = true
           or t.chave_paciente is not null

    ),

    -- Cria a flag que indica se o paciente possui CPF preenchido
    paciente_com_flag as (

        select
            *,
            case
                when cpf_paciente is not null then true
                else false
            end as paciente_cpf_indicador
        from pacientes_elegiveis

    ),

    -- Deduplica o vínculo do paciente na mesma equipe, priorizando:
    -- permanente > vínculo mais recente > atualização cadastral mais recente
    ultimo_vinculo_paciente as (

        select *
        from paciente_com_flag
        qualify row_number() over (
            partition by id_paciente, id_ine
            order by
                cadastro_permanente_indicador desc,
                data_atualizacao_vinculo_equipe desc nulls last,
                data_ultima_atualizacao_cadastral desc nulls last,
                source_updated_at desc nulls last,
                updated_at_rank desc nulls last
        ) = 1

    ),

    profissionais_medicos as (

        select
            e.id_ine,
            medico as id_profissional_sus,
            'medico' as tipo_profissional
        from {{ ref('dim_equipe') }} e
        cross join unnest(e.medicos) as medico

    ),

    profissionais_enfermeiros as (

        select
            e.id_ine,
            enfermeiro as id_profissional_sus,
            'enfermeiro' as tipo_profissional
        from {{ ref('dim_equipe') }} e
        cross join unnest(e.enfermeiros) as enfermeiro

    ),

    profissionais_equipe as (

        select * from profissionais_medicos
        union all
        select * from profissionais_enfermeiros

    ),

    -- Enriquece os profissionais da equipe com CPF, CNS, nome e indicador de ativo
    profissionais_equipe_enriquecido as (

        select
            pe.id_ine,
            pe.id_profissional_sus,
            {{ process_null('ps.cpf') }} as cpf_profissional,
            {{ process_null('ps.cns') }} as cns_profissional,
            {{ proper_br('ps.nome') }} as nome_profissional,
            pe.tipo_profissional,
            ps.funcionario_ativo_indicador
        from profissionais_equipe pe
        left join {{ ref('dim_profissional_saude') }} ps
            on pe.id_profissional_sus = ps.id_profissional_sus

    ),

    -- Remove duplicidades de profissionais na mesma equipe e descarta
    -- profissionais sem CPF, priorizando vínculos ativos
    profissionais_equipe_deduplicado as (

        select *
        from profissionais_equipe_enriquecido
        where id_ine is not null
          and cpf_profissional is not null
        qualify row_number() over (
            partition by id_ine, cpf_profissional
            order by
                funcionario_ativo_indicador desc nulls last,
                tipo_profissional
        ) = 1

    ),

    -- Enriquece o paciente com informações da equipe e da unidade
    equipe_enriquecida as (

        select
            p.id_paciente,
            p.cpf_paciente,
            p.cns_paciente,
            {{ proper_br('p.nome_paciente') }} as nome_paciente,
            {{ proper_br('p.mae_nome') }} as mae_nome,
            p.data_nascimento,
            p.situacao,
            p.paciente_cpf_indicador,
            p.id_ine,
            {{ proper_br('e.nome_referencia') }} as nome_equipe_familia,
            coalesce(e.id_cnes, p.id_cnes) as id_cnes,
            {{ proper_estabelecimento('est.nome_limpo') }} as nome_unidade,
            p.data_atualizacao_vinculo_equipe,
            p.data_ultima_atualizacao_cadastral,
            p.source_updated_at,
            p.updated_at_rank,
            p.cadastro_permanente_indicador
        from ultimo_vinculo_paciente p
        left join {{ ref('dim_equipe') }} e
            on p.id_ine = e.id_ine
        left join {{ ref('dim_estabelecimento') }} est
            on coalesce(e.id_cnes, p.id_cnes) = est.id_cnes

    ),


    profissional_paciente as (

        select
            ee.id_paciente,
            ee.cpf_paciente,
            ee.cns_paciente,
            ee.nome_paciente,
            ee.mae_nome,
            ee.data_nascimento,
            ee.situacao,
            ee.paciente_cpf_indicador,
            pe.id_ine,
            ee.nome_equipe_familia,
            ee.id_cnes,
            ee.nome_unidade,
            pe.cpf_profissional,
            pe.cns_profissional,
            pe.nome_profissional,
            pe.id_profissional_sus,
            pe.tipo_profissional,
            pe.funcionario_ativo_indicador,
            ee.data_atualizacao_vinculo_equipe,
            ee.data_ultima_atualizacao_cadastral,
            ee.source_updated_at,
            ee.updated_at_rank,
            ee.cadastro_permanente_indicador
        from equipe_enriquecida ee
        inner join profissionais_equipe_deduplicado pe
            on ee.id_ine = pe.id_ine

    )

select
    id_paciente,
    cpf_paciente,
    cns_paciente,
    nome_paciente,
    mae_nome,
    data_nascimento,
    situacao,
    cadastro_permanente_indicador,
    paciente_cpf_indicador,
    id_ine,
    nome_equipe_familia,
    id_cnes,
    nome_unidade,
    cpf_profissional,
    cns_profissional,
    nome_profissional,
    id_profissional_sus,
    tipo_profissional,
    funcionario_ativo_indicador,
    data_atualizacao_vinculo_equipe,
    data_ultima_atualizacao_cadastral,
    source_updated_at,
    updated_at_rank
from profissional_paciente