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
            especialidade_nome,
            case
                when regexp_replace(cns, '[^0-9]', '') = ''
                then null
                else regexp_replace(cns, '[^0-9]', '')
            end as cns,
            case
                when regexp_replace(cpf, '[^0-9]', '') = ''
                then null
                else regexp_replace(cpf, '[^0-9]', '')
            end as cpf,
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
            if(cid_codigo in ('None', ''), null, cid_codigo) as cid_codigo,
            if(cid_nome in ('None', ''), null, cid_nome) as cid_nome,

        from {{ ref("int_historico_clinico__atendimento__vitai") }}
    ),
    all_queixa as (
        select
            gid_boletim,
            queixa,
            inicio_datahora,
            row_number() over (
                partition by gid_boletim order by inicio_datahora desc
            ) as ordenacao
        from {{ ref("int_historico_clinico__atendimento__vitai") }}
    ),
    last_queixa as (select gid_boletim, queixa from all_queixa where ordenacao = 1),
    profissional as (
        select gid, cns, cpf, nome, cbo_descricao
        from {{ ref("int_historico_clinico__profissional_saude__vitai") }}
    ),
    estabelecimentos as (
        select
            gid, cnes, nome_estabelecimento, estabelecimento_dim.tipo_sms_simplificado
        from
            {{ ref("int_historico_clinico__estabelecimento__vitai") }}
            as estabelecimento_vitai
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
                when (atendimento.cid_codigo is null) and (atendimento.cid_nome is null)
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
            array_agg(
                struct(cid_id as id, cid_nome as descricao) ignore nulls
            ) as condicoes,
            max(episodio_informativo) as episodio_informativo
        from cid_distinct
        group by 1
    ),
    profissional_distinct as (
        select distinct
            concat(estabelecimentos.cnes, ".", boletim.gid) as id,
            atendimento.gid_profissional as profissional_id,
            case
                when regexp_replace(profissional.cpf, '[^0-9]', '') = ''
                then null
                else regexp_replace(profissional.cpf, '[^0-9]', '')
            end as profissional_cpf,
            case
                when regexp_replace(profissional.cns, '[^0-9]', '') = ''
                then null
                else regexp_replace(profissional.cns, '[^0-9]', '')
            end as profissional_cns,
            if(
                profissional.nome = 'None', null, profissional.nome
            ) as profissional_nome,
            profissional.cbo_descricao
        from boletim
        left join atendimento on boletim.gid = atendimento.gid_boletim
        left join estabelecimentos on boletim.gid_estabelecimento = estabelecimentos.gid
        left join profissional on atendimento.gid_profissional = profissional.gid
    ),
    profissional_grouped as (
        select
            id,
            struct(
                profissional_id as id,
                profissional_nome as nome,
                profissional_cpf as cpf,
                profissional_cns as cns,
                cbo_descricao as especialidade
            ) as profissional_saude_responsavel
        from profissional_distinct
        limit 1
    ),
    atendimento_struct as (
        select
            concat(estabelecimentos.cnes, ".", boletim.gid) as id,
            last_queixa.queixa as motivo_atendimento,
            trim(initcap(boletim.atendimento_tipo)) as tipo,
            trim(initcap(boletim.especialidade_nome)) as subtipo,
            case
                when (data_entrada in ("None", "NaT")) or (data_entrada is null)
                then null
                else cast(data_entrada as datetime)
            end as entrada_datahora,
            case
                when (alta_data in ("None", "NaT")) or (data_entrada is null)
                then null
                else cast(alta_data as datetime)
            end as saida_datahora,
            struct(boletim.gid as id_prontuario, boletim.cpf, boletim.cns) as paciente,
            struct(
                estabelecimentos.cnes as id_cnes,
                estabelecimentos.nome_estabelecimento as nome,
                estabelecimentos.tipo_sms_simplificado as estabelecimento_tipo
            ) as estabelecimento,
            struct(
                boletim.gid as id_atendimento,
                "vitai" as fornecedor
            ) as prontuario,
            imported_at,
            updated_at,
            case
                when (boletim.cpf is null) and (boletim.cns is null) then 0 else 1
            end as episodio_com_paciente

        from boletim
        left join estabelecimentos on boletim.gid_estabelecimento = estabelecimentos.gid
        left join last_queixa on boletim.gid = last_queixa.gid_boletim
    )
select
    -- Paciente
    atendimento_struct.paciente,

    -- Tipo e Subtipo
    safe_cast(atendimento_struct.tipo as string) as tipo,
    safe_cast(atendimento_struct.subtipo as string) as subtipo,

    -- Entrada e Saída
    safe_cast(atendimento_struct.entrada_datahora as datetime) as entrada_datahora,
    safe_cast(atendimento_struct.saida_datahora as datetime) as saida_datahora,

    -- Motivo e Desfecho
    safe_cast(atendimento_struct.motivo_atendimento as string) as motivo_atendimento,
    safe_cast(null as string) as desfecho_atendimento,

    -- Condições
    cid_grouped.condicoes,

    -- Estabelecimento
    atendimento_struct.estabelecimento,

    -- Profissional
    profissional_grouped.profissional_saude_responsavel,

    -- Prontuário
    atendimento_struct.prontuario,

    -- Metadados
    struct(
        safe_cast(imported_at as datetime) as loaded_at,
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(current_datetime() as datetime) as processed_at,
        safe_cast(
            case
                when
                    (
                        (cid_grouped.episodio_informativo = 0)
                        and (atendimento_struct.motivo_atendimento is null)
                    )
                    or (
                        (atendimento_struct.tipo is null)
                        and (atendimento_struct.subtipo is null)
                    )
                    or (atendimento_struct.entrada_datahora is null)
                then 0
                else 1
            end 
        as boolean) as tem_informacoes_basicas,
        safe_cast(
            atendimento_struct.episodio_com_paciente 
        as boolean) as tem_identificador_paciente,
        safe_cast(
            null 
        as boolean) as tem_informacoes_sensiveis
    ) as metadados
from atendimento_struct
    left join cid_grouped 
        on atendimento_struct.id = cid_grouped.id
    left join profissional_grouped 
        on atendimento_struct.id = profissional_grouped.id
