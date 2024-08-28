{{
    config(
        schema="intermediario_historico_clinico",
        alias="episodio_assistencial_vitai",
        materialized="table",
    )
}}
-- Cria tabela padronizada da entidade episodio assistencial da vitai 
with
    -- Traz boletins com chaves de paciente tratadas
    boletim as (
        select
            gid,
            gid_paciente,
            gid_estabelecimento,
            atendimento_tipo,
            especialidade_nome,
            case
                when {{process_null('internacao_data')}} is null then null
                else cast(internacao_data as datetime)
            end as internacao_data,
            {{clean_numeric('cns')}} as cns,
            {{clean_numeric('cpf')}} as cpf,
            imported_at,
            updated_at,
            case
                when {{process_null('data_entrada')}} is null then null
                else cast(data_entrada as datetime)
            end as entrada_datahora,
            case
                when {{process_null('alta_data')}} is null then null
                else cast(alta_data as datetime)
            end as saida_datahora,
        from {{ ref("raw_prontuario_vitai__boletim") }}
    ),
    consulta as (
        select
            boletim.*,
            'Consulta' as tipo,
            atendimento.gid_profissional,
            {{process_null('atendimento.cid_codigo')}} as cid_codigo,
            {{process_null('atendimento.cid_nome')}}  as cid_nome,
            CASE 
                WHEN trim(lower(boletim.atendimento_tipo)) = 'emergencia' THEN 'Emergência'
                WHEN trim(lower(boletim.atendimento_tipo)) = 'consulta' THEN 'Ambulatorial'
                ELSE null
            END  as subtipo,
            array(
                select as struct 
                cast(null as string) as tipo,
                cast(null as string) as descricao
            ) as exames_realizados
        from boletim
        left join
            {{ ref("raw_prontuario_vitai__atendimento") }} as atendimento
            on boletim.gid = atendimento.gid_boletim
        where atendimento.gid_boletim is not null and boletim.internacao_data is null
    ),
    -- Alguns hospitais podem por pacientes em obs antes de internar, gerando duplicadas na tabela. Vale o ultimo registros nesse caso
    internacao_all as (
        select
            gid_boletim,  
            internacao_tipo,
            {{process_null('internacao.id_diagnostico')}} as cid_codigo,
            {{process_null('internacao.diagnostico_descricao')}} as cid_nome,
            row_number() over (
                partition by gid_boletim order by internacao_data desc
            ) as ordenacao
        from {{ ref("raw_prontuario_vitai__internacao") }} 
    ),
    internacao as (
        select
            boletim.*,
            'Internação' as tipo,
            safe_cast(null as string) as gid_profissional,
            internacao_distinct.cid_codigo,
            internacao_distinct.cid_nome,
            CASE 
                WHEN trim(lower(internacao_distinct.internacao_tipo)) = 'emergencia' THEN 'Emergência'
                ELSE trim(initcap(internacao_distinct.internacao_tipo)) 
            END as subtipo,
            array(
                select as struct 
                cast(null as string) as tipo,
                cast(null as string) as descricao
            ) as exames_realizados
        from boletim
        left join ( select * from internacao_all where ordenacao=1) internacao_distinct
            on boletim.gid = internacao_distinct.gid_boletim
        where internacao_distinct.gid_boletim is not null and boletim.internacao_data is not null
    ),
    -- Monta relação de exames em cada episódio, retirando duplicadas de exames refeitos e agrupando episodios com exames de imagem e laboratorio
    -- como um só
    exame_dupl as (
        select
            boletim.*,
            'Exame' as tipo,
            exame_table.exame_descricao,
            safe_cast(null as string) as gid_profissional,
            safe_cast(null as string) as cid_codigo,
            safe_cast(null as string) as cid_descricao,
            CASE 
                WHEN trim(lower(exame_table.tipo)) = 'laboratorio' THEN 'Laboratório'
                ELSE trim(initcap(exame_table.tipo)) 
            END as subtipo
        from boletim
        left join (select distinct gid_boletim, tipo, exame_descricao  from {{ref("raw_prontuario_vitai__exame")}} ) as exame_table
            on boletim.gid = exame_table.gid_boletim
        left join
            {{ ref("raw_prontuario_vitai__atendimento") }} as atendimento
            on boletim.gid = atendimento.gid_boletim
        where
            exame_table.gid_boletim is not null
            and atendimento.gid_boletim is null
            and boletim.internacao_data is null
    ),
    exame as (
        select
            gid,
            gid_paciente,
            gid_estabelecimento,
            atendimento_tipo,
            especialidade_nome,
            internacao_data,
            cns,
            cpf,
            imported_at,
            updated_at,
            entrada_datahora,
            saida_datahora,
            tipo,
            gid_profissional,
            cid_codigo,
            cid_descricao,
            array_to_string(
                array_agg(distinct subtipo order by subtipo desc),
                ' e ') as subtipo,
            array_agg(
                struct( 
                    cast(subtipo as string) as tipo,
                    cast(exame_descricao as string) as descricao
                )
            ) as exames_realizados
        from exame_dupl
        group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16
    ),
    episodios as (
        select *
        from consulta
        union all
        select *
        from internacao
        union all
        select *
        from exame
    ),
    -- Como cada atendimento appenda informações no boletim, pegamos a queixa do
    -- ultimo atendimento
    queixa_all as (
        select
            gid_boletim,
            queixa,
            inicio_datahora,
            gid_profissional,
            row_number() over (
                partition by gid_boletim order by inicio_datahora desc
            ) as ordenacao
        from {{ ref("raw_prontuario_vitai__atendimento") }}
    ),
    queixa_final as (
        select
            gid_boletim,
            gid_profissional,
            case
                when {{process_null('queixa')}} is null then null
                else upper(trim(queixa))
            end as queixa
        from queixa_all
        where ordenacao = 1
    ),
     -- Desfecho do atendimento
    desfecho_atendimento_all as (
        select
            gid_boletim,
            case
                when {{process_null('resumo_alta_descricao')}} is null and {{process_null('desfecho_internacao')}} is  null
                then null
                when {{process_null('resumo_alta_descricao')}} is null and {{process_null('desfecho_internacao')}} is not null
                then upper(desfecho_internacao)
                else concat(upper(desfecho_internacao),'\n',upper(trim(resumo_alta_descricao)))
            end as desfecho,
            row_number() over (
                partition by gid_boletim order by resumo_alta_datahora desc
            ) as ordenacao
        from {{ ref("raw_prontuario_vitai__resumo_alta") }}
    ),
    desfecho_atendimento_final as (
        select
            gid_boletim,
            REGEXP_REPLACE(desfecho,'[Ó|O]BITO {1,}\n[Ó|O]BITO','OBITO') as desfecho
        from desfecho_atendimento_all
        where ordenacao = 1
    ),
    -- Profissional com nome próprio tratado
    profissional_int as (
        select gid, cns, cpf, initcap(nome) as nome, cbo_descricao
        from {{ ref("raw_prontuario_vitai__profissional") }}
    ),
    profissional as (
        select gid, cns, cpf, {{ proper_br("nome") }} as nome, cbo_descricao
        from profissional_int
    ),
    -- Estabelecimento com infos da tabela mestre
    estabelecimentos as (
        select
            gid,
            cnes,
            estabelecimento_dim.nome_limpo as nome_estabelecimento,
            estabelecimento_dim.tipo_sms_simplificado
        from
            {{ ref("raw_prontuario_vitai__m_estabelecimento") }}
            as estabelecimento_vitai
        left join
            {{ ref("dim_estabelecimento") }} as estabelecimento_dim
            on estabelecimento_vitai.cnes = estabelecimento_dim.id_cnes
    ),
    -- Monta estrura array aninhada de CIDs do episódio
    cid_distinct as (
        select distinct
            episodios.gid as id,
            episodios.cid_codigo as cid_id,
            episodios.cid_nome as cid_nome,
            case
                when (episodios.cid_codigo is null) and (episodios.cid_nome is null)
                then 0
                else 1
            end as episodio_informativo
        from episodios
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
    -- Monta estrurra array aninhada de profissionais do episódio
    profissional_distinct as (
        select distinct
            queixa_final.gid_boletim as gid_boletim,
            queixa_final.gid_profissional as profissional_id,
            {{clean_numeric('profissional.cns')}} as profissional_cns,
            {{clean_numeric('profissional.cpf')}} as profissional_cpf,
            {{process_null('profissional.nome')}} as profissional_nome,
            profissional.cbo_descricao
        from queixa_final
        left join profissional on queixa_final.gid_profissional = profissional.gid
    ),
    -- Monta base do episódio para ser enriquecida
    atendimento_struct as (
        select
            episodios_distinct.gid as id,
            queixa_final.queixa as motivo_atendimento,
            episodios_distinct.tipo,
            episodios_distinct.subtipo,
            episodios_distinct.exames_realizados,
            desfecho_atendimento_final.desfecho,
            episodios_distinct.entrada_datahora,
            episodios_distinct.saida_datahora,
            struct(episodios_distinct.gid as id_prontuario, episodios_distinct.cpf, episodios_distinct.cns) as paciente,
            struct(
                    profissional_distinct.profissional_id as id,
                    profissional_distinct.profissional_cpf as cpf,
                    profissional_distinct .profissional_cns as cns,
                    {{ proper_br('profissional_nome') }} as nome,
                    profissional_distinct.cbo_descricao as especialidade
                ) as profissional_saude_responsavel,
            struct(
                estabelecimentos.cnes as id_cnes,
                {{ proper_estabelecimento("nome_estabelecimento") }} as nome,
                estabelecimentos.tipo_sms_simplificado as estabelecimento_tipo
            ) as estabelecimento,
            struct(episodios_distinct.gid as id_atendimento, "vitai" as fornecedor) as prontuario,
            episodios_distinct.imported_at,
            episodios_distinct.updated_at,
            case
                when (episodios_distinct.cpf is null) and (episodios_distinct.cns is null) then 0 else 1
            end as episodio_com_paciente

        from (
            select distinct gid, 
            gid_estabelecimento,
            tipo,
            subtipo,
            exames_realizados,
            entrada_datahora,
            saida_datahora,
            imported_at,
            updated_at,
            cpf,
            cns 
            from episodios
        ) as episodios_distinct
        left join estabelecimentos on episodios_distinct.gid_estabelecimento = estabelecimentos.gid
        left join queixa_final on episodios_distinct.gid = queixa_final.gid_boletim
        left join
            desfecho_atendimento_final
            on episodios_distinct.gid = desfecho_atendimento_final.gid_boletim
        left join profissional_distinct on episodios_distinct.gid = profissional_distinct.gid_boletim
    )
    select    
    -- Paciente
    paciente_struct.paciente,

    -- Tipo e Subtipo
    safe_cast(atendimento_struct.tipo as string) as tipo,
    safe_cast(atendimento_struct.subtipo as string) as subtipo,
    exames_realizados,

    -- Entrada e Saída
    safe_cast(atendimento_struct.entrada_datahora as datetime) as entrada_datahora,
    safe_cast(atendimento_struct.saida_datahora as datetime) as saida_datahora,

    -- Motivo e Desfecho
    safe_cast(atendimento_struct.motivo_atendimento as string) as motivo_atendimento,
    safe_cast(desfecho as string) as desfecho_atendimento,
    
    -- Condições
    cid_grouped.condicoes,

    -- Estabelecimento
    atendimento_struct.estabelecimento,

    -- Profissional
    atendimento_struct.profissional_saude_responsavel,

    -- Prontuário
    atendimento_struct.prontuario,

    -- Metadados
    struct(
        safe_cast(updated_at as datetime) as updated_at,
        safe_cast(imported_at as datetime) as loaded_at,
        safe_cast(current_datetime() as datetime) as processed_at
    ) as metadados
    from atendimento_struct
    left join cid_grouped on atendimento_struct.id = cid_grouped.id