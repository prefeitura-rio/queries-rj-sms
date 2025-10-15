{{
  config(
    alias="episodios",
    schema="informes_seguranca",
    materialized="incremental",
    incremental_strategy="merge",
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "day",
    },
  )
}}


with

  -- Dados de episódios assistenciais da Vitai e Vitacare
  -- Aqui só estamos interessados em alguns poucos campos
  -- - CPF do paciente
  -- - Data/hora de entrada e saída
  -- - CIDs
  -- - Unidade de saúde
  -- - ID do prontuário
  merged_data as (
    select
      id_hci,
      cpf,
      entrada_datahora,
      saida_datahora,
      condicoes,
      estabelecimento,
      prontuario,
      metadados,
      cpf_particao,
    from {{ ref("int_historico_clinico__episodio__vitai") }}
    {% if is_incremental() %}
      where date(metadados.imported_at) > (select max(data_particao) from {{ this }})
    {% endif %}

    union all

    select
      id_hci,
      cpf,
      entrada_datahora,
      saida_datahora,
      condicoes,
      estabelecimento,
      prontuario,
      metadados,
      cpf_particao,
    from {{ ref("int_historico_clinico__episodio__vitacare") }}
    {% if is_incremental() %}
      where date(metadados.imported_at) > (select max(data_particao) from {{ this }})
    {% endif %}
  ),

  -- Aqui pegamos nome e data de nascimento de pacientes que temos cadastrados
  merged_patient as (
    select
      cpf,
      cns,
      dados.nome,
      dados.nome_social,
      dados.data_nascimento,
    from {{ ref("mart_historico_clinico__paciente") }}
  ),
  -- Popula dados de nome e data de nascimento nos CPFs com episódios assistenciais
  -- É possível (e inevitável) que alguns registros sigam sem informações, normalmente
  -- por não preenchimento de CPF
  merged_data_patient as (
    select
      merged_data.* except (cpf),
      struct(
        coalesce(merged_data.cpf, merged_patient.cpf),
        merged_patient.cns,
        merged_patient.nome,
        merged_patient.nome_social,
        merged_patient.data_nascimento,
        {{
          dbt_utils.generate_surrogate_key([
            "merged_patient.cpf"
          ])
        }} as id_paciente
      ) as paciente,
    from merged_data
    left join merged_patient
      on merged_patient.cpf = merged_data.cpf
  ),

  -- Deduplica dados com mesmo ID
  deduped as (
    select *
    from merged_data_patient
    qualify row_number() over (partition by id_hci) = 1
  ),
  -- Abre struct de CIDs
  deduped_unnested as (
    select
      deduped.* except(condicoes),
      ep_cid.id,
      ep_cid.descricao,
      ep_cid.situacao,
      ep_cid.data_diagnostico
    from deduped,
      unnest(deduped.condicoes) as ep_cid
  ),

  -- Pega CIDs relevantes, especificados na planilha
  relevant_cids as (
    select cid
    from {{ ref("raw_sheets__seguranca_cids") }}
  ),

  -- Somente episódios com CIDs relevantes
  relevant_eps as (
    select
      deduped_unnested.prontuario.id_prontuario_global,
      array_agg(
        struct(
          deduped_unnested.id,
          deduped_unnested.descricao,
          deduped_unnested.situacao,
          deduped_unnested.data_diagnostico
        )
        order by
          deduped_unnested.data_diagnostico desc,
          deduped_unnested.descricao asc
      ) as condicoes
    from relevant_cids
    inner join deduped_unnested
      on starts_with(
        regexp_replace(deduped_unnested.id, r"\.", ""),
        relevant_cids.cid
      )
    group by 1
  ),

  -- Popula descrições de CIDs
  eps_cid_subcat as (
    select
      relevant_eps.id_prontuario_global,
      cid.id,
      cid.descricao,
      cid.situacao,
      cid.data_diagnostico,
      best_agrupador as descricao_agg
    from relevant_eps,
      unnest(condicoes) as cid
    left join {{ ref("int_historico_clinico__cid_subcategoria") }} as agg_4_dig
      on agg_4_dig.id = regexp_replace(cid.id, r'\.', '')
    where char_length(regexp_replace(cid.id, r'\.', '')) = 4
  ),
  eps_cid_cat as (
    select
      relevant_eps.id_prontuario_global,
      cid.id,
      cid.descricao,
      cid.situacao,
      cid.data_diagnostico,
      best_agrupador as descricao_agg
    from relevant_eps,
      unnest(condicoes) as cid
    left join {{ ref("int_historico_clinico__cid_categoria") }} as agg_3_dig
      on agg_3_dig.id_categoria = regexp_replace(cid.id, r'\.', '')
    where char_length(regexp_replace(cid.id, r'\.', '')) = 3
  ),
  all_cids as (
    select
      id_prontuario_global,
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

  with_cids as (
    select
      deduped.id_hci,
      deduped.paciente,
      deduped.entrada_datahora,
      deduped.saida_datahora,
      all_cids.condicoes,
      deduped.estabelecimento,
      deduped.prontuario,
      deduped.metadados,
      cast(deduped.entrada_datahora as date) as data_particao
    from deduped
    left join all_cids
      on all_cids.id_prontuario_global = deduped.prontuario.id_prontuario_global
  ),

  final as (
    select
      ep.* except (condicoes, metadados, data_particao),
      condicao.id as cid,
      condicao.descricao as cid_descricao,
      condicao.situacao as cid_situacao,
      ep.metadados,
      ep.data_particao
    from with_cids as ep,
      unnest(ep.condicoes) as condicao
    qualify row_number() over (
      -- TODO: descobrir como deduplicar entradas parecidas
      partition by
        entrada_datahora,
        estabelecimento.id_cnes,
        paciente.cpf,
        cid
      order by saida_datahora desc
    ) = 1
    order by
      cid asc,
      coalesce(paciente.nome_social, paciente.nome) asc nulls last
  )

select *
from final



-- TODO:
-- - CNES não está sendo populado corretamente
-- - Conferir se os CIDs estão filtrando certinho
-- - Aglutinar atendimentos simultâneos
