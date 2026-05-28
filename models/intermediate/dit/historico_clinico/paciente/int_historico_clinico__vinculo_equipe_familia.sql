{{
    config(
        schema="intermediario_historico_clinico",
        alias="vinculo_equipe_familia",
        materialized="table",
        cluster_by=['cpf_profissional']
    )
}}

with
    atendimentos_elegiveis as (
        select
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

    -- Ordena os atendimentos por paciente ao longo do tempo e também por paciente+unidade
    -- para identificar blocos contínuos de atendimento na mesma unidade
    atendimentos_ordenados as (

        select
            cpf_paciente,
            id_cnes,
            data_atendimento,
            cbo_3,
            cbo_4,
            cbo_descricao,
            row_number() over (
                partition by cpf_paciente
                order by data_atendimento
            ) as rn_geral,
            row_number() over (
                partition by cpf_paciente, id_cnes
                order by data_atendimento
            ) as rn_unidade
        from atendimentos_elegiveis

    ),

    -- Agrupa os atendimentos em sequências contínuas na mesma unidade e conta,
    -- dentro de cada bloco, quantos atendimentos foram realizados com médico/enfermeiro
    sequencias_consultas as (

        select
            cpf_paciente,
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

    -- Mantém apenas os pacientes temporários que tiveram mais de 5 consultas
    -- com médico/enfermeiro em uma sequência contínua na mesma unidade
    pacientes_temporarios_elegiveis as (

        select distinct
            cpf_paciente,
            id_cnes
        from sequencias_consultas
        where qtd_consultas_seguidas > 5

    ),

    -- Base de pacientes com vínculo de equipe preenchido, mantém apenas pacientes com situacao 'Ativo',
    paciente_base as (
        select
            p.id_paciente_global,
            p.id_paciente_local,
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
            p.updated_at_rank
        from {{ ref('raw_prontuario_vitacare__paciente') }} p
        where {{ process_null('p.id_ine') }} is not null
          and p.situacao = 'Ativo'

    ),

    -- Mantém os pacientes elegíveis:
    -- 1) cadastro permanente entra direto
    -- 2) temporários entram apenas se forem elegíveis pela regra das consultas
    pacientes_elegiveis as (

        select
            p.*
        from paciente_base p
        left join pacientes_temporarios_elegiveis t
            on p.cpf_paciente = t.cpf_paciente
           and p.id_cnes = t.id_cnes
        where p.cadastro_permanente_indicador = true
           or t.cpf_paciente is not null

    ),

    -- Busca CNS que possui CPF preenchido para enriquecer pacientes sem CPF.
    indice_cpf as (
        select
            cast(cns_particao as string) as cns_paciente,
            cpf as cpf_indice
        from {{ ref('mart_historico_clinico_app__indice') }}
        where cns_particao is not null
        and cpf is not null
    ),

    -- Enriquece os pacientes elegíveis que nao tem CPF cadastrado via CNS
    pacientes_elegiveis_enriquecidos as (

        select
            p.* except (cpf_paciente),
            coalesce(p.cpf_paciente, i.cpf_indice) as cpf_paciente
        from pacientes_elegiveis p
        left join indice_cpf i
            on p.cns_paciente = i.cns_paciente

    ),

    -- Deduplica o vínculo do paciente na mesma equipe, priorizando:
    -- permanente > vínculo mais recente > atualização cadastral mais recente
    ultimo_vinculo_paciente as (

        select *
        from pacientes_elegiveis_enriquecidos
        qualify row_number() over (
            partition by id_paciente_global, id_ine
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
                funcionario_ativo_indicador desc nulls last
        ) = 1

    ),

    -- Enriquece o paciente com informações da equipe e da unidade.
    equipe_enriquecida as (

        select
            p.id_paciente_global,
            p.id_paciente_local,
            p.cpf_paciente,
            p.cns_paciente,
            {{ proper_br('p.nome_paciente') }} as nome_paciente,
            {{ proper_br('p.mae_nome') }} as mae_nome,
            p.data_nascimento,
            p.situacao,
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

    -- Gera uma linha por relação profissional-paciente dentro da equipe.
    profissional_paciente as (

        select
            ee.id_paciente_global,
            ee.id_paciente_local,
            ee.cpf_paciente,
            ee.cns_paciente,
            ee.nome_paciente,
            ee.mae_nome,
            ee.data_nascimento,
            ee.situacao,
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
    id_paciente_global,
    id_paciente_local,
    cpf_paciente,
    cns_paciente,
    nome_paciente,
    mae_nome,
    data_nascimento,
    situacao,
    cadastro_permanente_indicador,
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