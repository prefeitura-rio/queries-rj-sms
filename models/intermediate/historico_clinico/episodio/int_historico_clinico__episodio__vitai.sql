-- cria tabela padronizada da entidade episodio assistencial da vitai 
with
    boletim as (
        select gid, gid_paciente, gid_estabelecimento, atendimento_tipo, cns, cpf
        from {{ ref("raw_prontuario_vitai__boletim") }}
    ),
    atendimento as (
        select
            gid,
            gid_boletim,
            gid_estabelecimento,
            gid_paciente,
            gid_profissional,
            atendimento_tipo,
            especialidade,
            inicio_datahora,
            fim_datahora,
            cid_codigo,
            cid_nome
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
        select gid, cnes, nome_estabelecimento, sigla
        from {{ ref("int_historico_clinico__estabelecimento__vitai") }}
    )
select
    concat(estabelecimentos.cnes, ".", atendimento.gid) as id,
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
    inicio_datahora as entrada_datahora,
    fim_datahora as saida_datahora,
    array_agg(
        struct(atendimento.gid_boletim as id_prontuario, boletim.cpf, boletim.cns)
    ) as paciente,
    array_agg(struct(cid_codigo as id, cid_nome as descricao)) as cid,
    array_agg(distinct alergias.descricao ignore nulls) as alergia,
    array_agg(
        struct(
            estabelecimentos.cnes as id_cnes,
            estabelecimentos.nome_estabelecimento as nome,
            estabelecimentos.sigla as estabelecimento_tipo
        )
    ) as estabelecimento,
    array_agg(
        struct(
            atendimento.gid_profissional as id,
            profissional.cpf,
            profissional.cns,
            profissional.nome
        )
    ) as profissional_saude_responsavel,
    array_agg(
        struct("vitai" as fornecedor, atendimento.gid as id_atendimento)
    ) as prontuario

from atendimento
left join boletim on boletim.gid = atendimento.gid_boletim
left join profissional on atendimento.gid_profissional = profissional.gid
left join alergias on atendimento.gid_boletim = alergias.gid_boletim
left join estabelecimentos on atendimento.gid_estabelecimento = estabelecimentos.gid
group by 1, 2, 3, 4, 5
