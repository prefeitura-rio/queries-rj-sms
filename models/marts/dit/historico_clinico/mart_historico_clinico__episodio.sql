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
            id_hci,
            cpf as paciente_cpf,
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
            id_hci,
            cpf as paciente_cpf,
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
            array<struct<nome string, quantidade integer, unidade_medica string, uso string, via_administracao string, prescricao_data timestamp>>[] as medicamentos_administrados,
            estabelecimento,
            profissional_saude_responsavel,
            prontuario,
            metadados,
            cpf_particao,
        from {{ ref("int_historico_clinico__episodio__vitacare") }}
    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- PATIENT DATA: Patient Enrichment 
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    merged_patient as (
        select cpf, cns, dados.data_nascimento
        from {{ref('mart_historico_clinico__paciente')}}
    ),
    merged_data_patient as (
        select merged_data.*,
            struct(
                merged_patient.cpf,
                merged_patient.cns,
                {{
                dbt_utils.generate_surrogate_key(
                        [
                            "cpf",
                        ]
                    )
                }} as id_paciente,
                merged_patient.data_nascimento
            ) as paciente,
        from merged_data
        left join merged_patient
        on merged_patient.cpf = merged_data.paciente_cpf
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
        from merged_data_patient
        left join
            deceased on merged_data_patient.prontuario.id_prontuario_global = deceased.boletim_obito

    ),
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- CONFLICT: Adding registration conflict flag
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    registration_conflict as (
        select * from {{ref('int_historico_clinico__pacientes_invalidos')}}
    ),

    deduped as (
        select *
        from merged_data_deceased
        qualify row_number() over (partition by id_hci) = 1
    ),

    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    -- CID: Add CID summarization
    -- -=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--=--
    eps_cid_subcat as (
        select
            deduped.prontuario.id_prontuario_global,
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
            deduped.prontuario.id_prontuario_global,
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
            id_prontuario_global,
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
        deduped.id_hci,
        deduped.paciente_cpf,
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
    on all_cids.id_prontuario_global = deduped.prontuario.id_prontuario_global
)
select *
from final

{% if is_incremental() %} where data_particao >= {{ partitions_to_replace }} {% endif %}
