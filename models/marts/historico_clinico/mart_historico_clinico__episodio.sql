{{
    config(
        schema="saude_historico_clinico",
        alias="episodio_assistencial",
        materialized="table",
        cluster_by="paciente_cpf",
        partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        },
    )
}}


with
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- MERGING DATA: Merging Data from Different Sources
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    merged_data as (
        select
            paciente,
            tipo,
            subtipo,
            entrada_datahora,
            saida_datahora,
            exames_realizados,
            motivo_atendimento,
            desfecho_atendimento,
            condicoes,
            null as prescricoes,  -- VITAI source does not have prescription data
            estabelecimento,
            profissional_saude_responsavel,
            prontuario,
            metadados,
            cpf_particao,
        from {{ ref("int_historico_clinico__episodio__vitai") }}
        union all
        select
            paciente,
            tipo,
            subtipo,
            entrada_datahora,
            saida_datahora,
            array(
                select as struct
                    cast(null as string) as tipo, cast(null as string) as descricao
            ) as exames_realizados,
            motivo_atendimento,
            desfecho_atendimento,
            condicoes,
            prescricoes,
            estabelecimento,
            profissional_saude_responsavel,
            prontuario,
            metadados,
            cpf_particao,
        from {{ ref("int_historico_clinico__episodio__vitacare") }}
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- FINGERPRINT: Adding Unique Hashed Field
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    fingerprinted as (
        select
            -- Patient Unique Identifier: for clustering purposes
            paciente.cpf as paciente_cpf,

            -- Encounter Unique Identifier: for testing purposes
            farm_fingerprint(
                concat(prontuario.fornecedor, prontuario.id_atendimento)
            ) as id_episodio,

            -- Encounter Data
            merged_data.*,
        from merged_data
    ),
    deduped as (
        select *
        from fingerprinted
        qualify row_number() over (partition by id_episodio) = 1
    ),

    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- CID: Add CID summarization
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    eps_cid_subcat as (
        select  
            id_episodio, 
            data_diagnostico, 
            {{clean_cid('best_agrupador')}} as descricao_agg
        from  deduped, unnest(condicoes) as cid 
        LEFT JOIN {{ ref("int_historico_clinico__cid_subcategoria") }} as agg_4_dig
        on agg_4_dig.id_subcategoria = regexp_replace(cid.id,r'\.','')
        where char_length(regexp_replace(cid.id,r'\.','')) = 4
        and cid.situacao != 'RESOLVIDO'
    ),
    eps_cid_cat as (
        select 
            id_episodio, 
            data_diagnostico, 
            {{clean_cid('best_agrupador')}} as descricao_agg
        from  deduped, unnest(condicoes) as cid 
        LEFT JOIN {{ ref("int_historico_clinico__cid_categoria") }} as agg_3_dig
        on agg_3_dig.id_categoria = regexp_replace(cid.id,r'\.','')
        where char_length(regexp_replace(cid.id,r'\.','')) = 3
        and cid.situacao != 'RESOLVIDO'
    ),
    all_cids as (
    select distinct id_episodio, data_diagnostico, descricao_agg
    from (
        select * from eps_cid_subcat where descricao_agg != ''
        union all
        select * from eps_cid_cat where descricao_agg != ''
    )
    qualify row_number() over (partition by id_episodio,descricao_agg order by data_diagnostico desc) = 1
    ),
    summarization as (
        select 
            id_episodio, 
            array_agg(descricao_agg ignore nulls order by data_diagnostico desc) as condicoes_resumo
        from all_cids
        group by 1
    )

    select 
        deduped.paciente_cpf,
        deduped.id_episodio,
        deduped.paciente,
        deduped.tipo,
        deduped.subtipo,
        deduped.entrada_datahora,
        deduped.saida_datahora,
        deduped.exames_realizados,
        deduped.motivo_atendimento,
        deduped.desfecho_atendimento,
        deduped.condicoes,
        summarization.condicoes_resumo,
        deduped.prescricoes,
        deduped.estabelecimento,
        deduped.profissional_saude_responsavel,
        deduped.prontuario,
        deduped.metadados,
        deduped.cpf_particao, 
    from deduped
    left join summarization
    on deduped.id_episodio = summarization.id_episodio



