{{
    config(
        schema="saude_historico_clinico",
        alias="episodio_assistencial",
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
    )
}}

{% set partitions_to_replace = (
    "date_sub(current_date('America/Sao_Paulo'), interval 30 day)"
) %}

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
            array(
                select as struct
                    cast(null as string) as descricao, cast(null as string) as observacao
            ) as procedimentos_realizados,
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
            procedimentos_realizados,
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
    -- DECEASED: Adding deceased flag
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    deceased as (
        select boletim_obito
        from {{ref('int_historico_clinico__obito__vitai')}}, unnest(gid_boletim_obito) as boletim_obito    
    ),
    merged_data_deceased as (
        select *, IF(deceased.boletim_obito is null, False, True) as obito_indicador
        from merged_data
        left join deceased
        on merged_data.prontuario.id_atendimento = deceased.boletim_obito

    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- FINGERPRINT: Adding Unique Hashed Field
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    fingerprinted as (
        select
            -- Patient Unique Identifier: for clustering purposes
            paciente.cpf as paciente_cpf,

            -- Encounter Unique Identifier: for testing purposes
            {{
                dbt_utils.generate_surrogate_key(
                    [
                        "prontuario.id_atendimento",
                    ]
                )
            }} as id_episodio,

            -- Encounter Data
            merged_data_deceased.*,
        from merged_data_deceased
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
            cid.id,
            cid.descricao,
            cid.situacao,
            cid.data_diagnostico, 
            best_agrupador as descricao_agg
        from  deduped, unnest(condicoes) as cid 
        LEFT JOIN {{ ref("int_historico_clinico__cid_subcategoria") }} as agg_4_dig
        on agg_4_dig.id = regexp_replace(cid.id,r'\.','')
        where char_length(regexp_replace(cid.id,r'\.','')) = 4
    ),
    eps_cid_cat as (
        select 
            id_episodio, 
            cid.id,
            cid.descricao,
            cid.situacao,
            cid.data_diagnostico,
            best_agrupador  as descricao_agg
        from  deduped, unnest(condicoes) as cid 
        LEFT JOIN {{ ref("int_historico_clinico__cid_categoria") }} as agg_3_dig
        on agg_3_dig.id_categoria = regexp_replace(cid.id,r'\.','')
        where char_length(regexp_replace(cid.id,r'\.','')) = 3
    ),
    all_cids as (
    select     
        id_episodio,
        array_agg(
            struct(
                id, 
                descricao, 
                situacao,
                data_diagnostico, 
                descricao_agg as resumo
            )
            order by data_diagnostico desc, descricao
        ) as condicoes
    from (
        select * from eps_cid_subcat
        union all
        select * from eps_cid_cat
    )
    group by 1
    ),
    final as (
    select 
        deduped.paciente_cpf,
        deduped.id_episodio,
        deduped.paciente,
        deduped.tipo,
        deduped.subtipo,
        cast(deduped.entrada_datahora as date) as entrada_data,
        deduped.entrada_datahora,
        deduped.saida_datahora,
        deduped.exames_realizados,
        deduped.procedimentos_realizados,
        deduped.motivo_atendimento,
        deduped.desfecho_atendimento,
        deduped.obito_indicador,
        all_cids.condicoes,
        deduped.prescricoes,
        deduped.estabelecimento,
        deduped.profissional_saude_responsavel,
        deduped.prontuario,
        deduped.metadados,
        cast(deduped.entrada_datahora as date) as data_particao
    from deduped
    left join all_cids 
    on all_cids.id_episodio = deduped.id_episodio
)

select * from final

{% if is_incremental() %}
    where data_particao >= {{ partitions_to_replace }}
{% endif %}
