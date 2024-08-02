{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_vitai",
        materialized="table",
    )
}}
-- cria tabela padronizada da entidade episodio assistencial da vitai 
with
    boletim as (
        select
            gid,
            gid_paciente,
            gid_estabelecimento,
            atendimento_tipo,
            cns,
            cpf,
            imported_at,
            updated_at,
            alta_data,
            data_entrada
        from {{ ref("raw_prontuario_vitai__boletim") }}
    ),
    atendimento as (
        select
            gid,
            gid_boletim,
            gid_estabelecimento,
            gid_profissional,
            atendimento_tipo,
            especialidade,
            cid_codigo,
            cid_nome,

        from {{ ref("int_historico_clinico__atendimento__vitai") }}
    ),
    profissional as (
        select gid, cns, cpf, nome
        from {{ ref("int_historico_clinico__profissional_saude__vitai") }}
    ),
    alergias as (
        select gid, gid_boletim, descricao
        from {{ ref("int_historico_clinico__alergia__vitai") }}
    ),
    estabelecimentos as (
        select
            gid, cnes, nome_estabelecimento, estabelecimento_dim.tipo_sms_simplificado
        from {{ ref("raw_prontuario_vitai__m_estabelecimento") }} as estabelecimento_vitai
        left join
            {{ ref("dim_estabelecimento") }} as estabelecimento_dim
            on estabelecimento_vitai.cnes = estabelecimento_dim.id_cnes
    ),
    cid_distinct as (
        select distinct
            concat(estabelecimentos.cnes, ".", boletim.gid) as id,
            atendimento.cid_codigo as cid_id,
            atendimento.cid_nome as cid_nome,
            case
                when atendimento.cid_codigo is null and atendimento.cid_nome is null
                then 0
                else 1
            end as episodio_informativo
        from boletim
        left join atendimento on boletim.gid = atendimento.gid_boletim
        left join estabelecimentos on boletim.gid_estabelecimento = estabelecimentos.gid
    ),
    cid_grouped as (
        select
            id,
            array_agg(struct(cid_id as id, cid_nome as descricao) ignore nulls) as cid,
            max(episodio_informativo) as episodio_informativo
        from cid_distinct
        group by 1
    ),
    profissional_distinct as (
        select distinct
            concat(estabelecimentos.cnes, ".", boletim.gid) as id,
            atendimento.gid_profissional as profissional_id,
            profissional.cpf as profissional_cpf,
            profissional.cns as profissional_cns,
            profissional.nome as profissional_nome
        from boletim
        left join atendimento on boletim.gid = atendimento.gid_boletim
        left join estabelecimentos on boletim.gid_estabelecimento = estabelecimentos.gid
        left join profissional on atendimento.gid_profissional = profissional.gid
    ),
    profissional_grouped as (
        select
            id,
            array_agg(
                struct(
                    profissional_id as id,
                    profissional_cpf as cpf,
                    profissional_cns as cns,
                    profissional_nome as nome
                ) ignore nulls
            ) as profissional_saude_responsavel
        from profissional_distinct
        group by 1
    ),
    alergias_grouped as (
        select
            concat(estabelecimentos.cnes, ".", boletim.gid) as id,
            array_agg(distinct descricao ignore nulls) as alergia,
        from boletim
        left join estabelecimentos on boletim.gid_estabelecimento = estabelecimentos.gid
        left join alergias on boletim.gid = alergias.gid_boletim
        group by 1
    ),
    atendimento_struct as (
        select
            concat(estabelecimentos.cnes, ".", boletim.gid) as id,
            case
                when
                    (trim(lower(boletim.atendimento_tipo)) = 'laboratorio')
                    or (trim(lower(boletim.atendimento_tipo)) = 'imagem')
                then 'Exames'
                when
                    (trim(lower(boletim.atendimento_tipo)) = 'consulta')
                    or (trim(lower(boletim.atendimento_tipo)) = 'emergencia')
                then 'Consulta'
                when trim(lower(boletim.atendimento_tipo)) = 'internacao'
                then 'Internação'
                else null
            end as tipo,
            case
                when lower(boletim.atendimento_tipo) = 'consulta'
                then 'Consulta Agendada'
                when lower(boletim.atendimento_tipo) = 'internação'
                then 'Cirurgia'
                when lower(boletim.atendimento_tipo) = 'nao informado'
                then null
                else trim(initcap(boletim.atendimento_tipo))
            end as subtipo,
            data_entrada as entrada_datahora,
            alta_data as saida_datahora,
            struct(boletim.gid as id_prontuario, boletim.cpf, boletim.cns) as paciente,
            struct(
                estabelecimentos.cnes as id_cnes,
                estabelecimentos.nome_estabelecimento as nome,
                estabelecimentos.tipo_sms_simplificado as estabelecimento_tipo
            ) as estabelecimento,
            struct("vitai" as fornecedor, boletim.gid as id_atendimento) as prontuario,
            struct(imported_at, updated_at) as metadados

        from boletim
        left join estabelecimentos on boletim.gid_estabelecimento = estabelecimentos.gid
    )
select
    atendimento_struct.id,
    atendimento_struct.tipo,
    atendimento_struct.subtipo,
    atendimento_struct.entrada_datahora,
    atendimento_struct.saida_datahora,
    cid_grouped.cid,
    alergias_grouped.alergia,
    profissional_grouped.profissional_saude_responsavel,
    atendimento_struct.estabelecimento,
    atendimento_struct.prontuario,
    atendimento_struct.metadados,
    cid_grouped.episodio_informativo
from atendimento_struct
left join cid_grouped on atendimento_struct.id = cid_grouped.id
left join alergias_grouped on atendimento_struct.id = alergias_grouped.id
left join profissional_grouped on atendimento_struct.id = profissional_grouped.id
