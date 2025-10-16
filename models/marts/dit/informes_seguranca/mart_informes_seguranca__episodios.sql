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
  -- - CPF do paciente / identificador do paciente (Vitai)
  -- - Data/hora de entrada e saída
  -- - CIDs
  -- - Unidade de saúde
  -- - ID do prontuário
  merged_data as (
    select
      id_hci,
      cpf,
      gid_paciente,
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
      gid_paciente,
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
      merged_data.* except (cpf, gid_paciente),
      struct(
        coalesce(merged_data.cpf, merged_patient.cpf) as cpf,
        merged_patient.cns as cns,
        merged_patient.nome as nome,
        merged_patient.nome_social as nome_social,
        merged_patient.data_nascimento as data_nascimento,
        coalesce(
          merged_data.gid_paciente,  -- Se tivermos um GID (Vitai), é preferível
          {{
            dbt_utils.generate_surrogate_key([
              "coalesce(merged_data.cpf, merged_patient.cpf)"
            ])
          }}
        ) as id_paciente
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

  -- Separa somente episódios com CIDs relevantes
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
  relevant_cids_with_description as (
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
      relevant_cids_with_description.condicoes,
      struct(
        deduped.estabelecimento.id_cnes,
        deduped.estabelecimento.nome
      ) as estabelecimento,
      deduped.prontuario,
      deduped.metadados,
      cast(deduped.entrada_datahora as date) as data_particao
    from relevant_cids_with_description
    left join deduped
      on relevant_cids_with_description.id_prontuario_global = deduped.prontuario.id_prontuario_global
  ),

  -- Queremos agrupar diferentes atendimentos em um só se tiverem:
  -- - mesmo paciente (via GID se Vitai, CPF se Vitacare);
  -- - mesma data de entrada;
  -- - mesmo CNES;
  -- - e mesmos CIDs
  -- Primeiro, juntamos todos os CIDs de cada atendimento em uma string
  grouped_cids as (
    select
      prontuario.id_prontuario_global,
      array_to_string(
        array(
          select cid.id
          from unnest(condicoes) as cid
          order by cid.id
        ), ";"
      ) as cids
    from with_cids
  ),
  -- Em seguida, criamos uma chave usando os campos descritos acima
  similar as (
    select
      with_cids.*,
      {{
        dbt_utils.generate_surrogate_key([
          "with_cids.paciente.id_paciente",
          "with_cids.data_particao",
          "with_cids.estabelecimento.id_cnes",
          "grouped_cids.cids",
        ])
      }} as id_similar,
    from with_cids
    left join grouped_cids
      on grouped_cids.id_prontuario_global = with_cids.prontuario.id_prontuario_global
  ),
  -- /!\  Existem casos em que o CPF é nulo! Diferentes pacientes atendidos em
  --      uma mesma unidade pra tratar um mesmo CID no mesmo dia podem então ser
  --      considerados um só.
  --      Meu plano aqui era verificar sobreposições no horário de entrada/saída
  --      (ex.: entrada1 <= saida2 && entrada2 <= saida1)
  --      Mas o CPF, o identificador que pode ser nulo, só é usado pela Vitacare,
  --      que não informa horários, só datas :|
  --         (* informa horários, mas às vezes sem entrada, então usamos só data)

  -- Numeramos cada linha com mesmo ID
  similar_numbered as (
    select
      *,
      row_number() over (
        partition by id_similar
        order by
          -- Prioridade para fins de atendimentos mais tardes, mas saída pode ser nulo
          saida_datahora desc,
          -- Prioridade secundária para IDs maiores, em teoria = atendimentos posteriores
          prontuario.id_prontuario_local desc
      ) as similar_row_number
    from similar
  ),
  deduped_similar as (
    select
      * except(id_similar, similar_row_number)
    from similar_numbered
    where similar_row_number = 1
  ),
  -- TODO: preservar número de prontuários de cada atendimento "duplicado"?

  final as (
    select
      ep.* except (condicoes, metadados, data_particao),

      -- Expande CIDs
      condicao.id as cid,
      condicao.descricao as cid_descricao,
      condicao.situacao as cid_situacao,

      -- Empurra metadados, data_particao para o final
      ep.metadados,
      ep.data_particao
    from deduped_similar as ep,
      unnest(ep.condicoes) as condicao
    order by
      cid asc,
      coalesce(paciente.nome_social, paciente.nome) asc nulls last
  )

select *
from final
