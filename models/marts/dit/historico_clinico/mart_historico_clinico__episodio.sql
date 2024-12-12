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
            null as procedimentos_realizados,
            motivo_atendimento,
            desfecho_atendimento,
            condicoes,
            struct(
                cast(null as float64) as altura,
                cast(null as float64) as circunferencia_abdominal,
                cast(null as float64) as frequencia_cardiaca,
                cast(null as float64) as frequencia_respiratoria,
                cast(null as float64) as glicemia,
                cast(null as float64) as hemoglobina_glicada,
                cast(null as float64) as imc,
                cast(null as float64) as peso,
                cast(null as float64) as pressao_sistolica,
                cast(null as float64) as pressao_diastolica,
                cast(null as string) as pulso_ritmo,
                cast(null as float64) as saturacao_oxigenio,
                cast(null as float64) as temperatura
            ) as medidas,
            array<struct<id string, nome string, concentracao string, uso_continuo string>>[] as prescricoes,
            medicamentos_administrados,
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
            medidas,
            prescricoes,
            array<struct<nome string, quantidade integer, unidade_medica string, uso string, via_administracao string>>[] as medicamentos_administrados,
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
        from
            {{ ref("int_historico_clinico__obito__vitai") }},
            unnest(gid_boletim_obito) as boletim_obito
    ),
    merged_data_deceased as (
        select *, if(deceased.boletim_obito is null, false, true) as obito_indicador
        from merged_data
        left join
            deceased on merged_data.prontuario.id_atendimento = deceased.boletim_obito

    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- CONFLICT: Adding registration conflict flag
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    registration_conflict as (
        select * from {{ref('int_historico_clinico__pacientes_invalidos')}}
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
        from deduped, unnest(condicoes) as cid
        left join
            {{ ref("int_historico_clinico__cid_subcategoria") }} as agg_4_dig
            on agg_4_dig.id = regexp_replace(cid.id, r'\.', '')
        where char_length(regexp_replace(cid.id, r'\.', '')) = 4
    ),
    eps_cid_cat as (
        select
            id_episodio,
            cid.id,
            cid.descricao,
            cid.situacao,
            cid.data_diagnostico,
            best_agrupador as descricao_agg
        from deduped, unnest(condicoes) as cid
        left join
            {{ ref("int_historico_clinico__cid_categoria") }} as agg_3_dig
            on agg_3_dig.id_categoria = regexp_replace(cid.id, r'\.', '')
        where char_length(regexp_replace(cid.id, r'\.', '')) = 3
    ),

    all_cids as (
        select
            id_episodio,
            array_agg(
                struct(
                    id, descricao, situacao, data_diagnostico, descricao_agg as resumo
                )
                order by data_diagnostico desc, descricao
            ) as condicoes
        from
            (
                select *
                from eps_cid_subcat
                union all
                select *
                from eps_cid_cat
            )
        group by 1
    ),

    final as (
    select 
        deduped.paciente_cpf,
        case 
            when deduped.paciente_cpf in (select cpf from registration_conflict) then true
            else false
        end as cadastros_conflitantes_indicador,
        deduped.id_episodio,
        deduped.paciente,
        deduped.tipo,
        deduped.subtipo,
        cast(deduped.entrada_datahora as date) as entrada_data,
        deduped.entrada_datahora,
        deduped.saida_datahora,
        deduped.exames_realizados,
        deduped.procedimentos_realizados,
        deduped.medidas,
        deduped.motivo_atendimento,
        deduped.desfecho_atendimento,
        deduped.obito_indicador,
        all_cids.condicoes,
        deduped.prescricoes,
        deduped.medicamentos_administrados,
        deduped.estabelecimento,
        deduped.profissional_saude_responsavel,
        deduped.prontuario,
        deduped.metadados,
        cast(deduped.entrada_datahora as date) as data_particao
    from deduped
    left join all_cids 
    on all_cids.id_episodio = deduped.id_episodio
)
select *
from final

{% if is_incremental() %} where data_particao >= {{ partitions_to_replace }} {% endif %}
