{{
  config(
    alias="atendimento",
    materialized="table",
    schema="projeto_ocis",
    meta={"owner": "avellar"}
  )
}}

with sarah_atendimento as (
  select distinct
    to_hex(sha1(atendimento_numero)) as id_atendimento,
    {{
      dbt_utils.generate_surrogate_key([
        "upper(regexp_replace(normalize(paciente_nome, NFD), r'[^\p{Letter}]', ''))",
        "paciente_data_nascimento"
      ])
    }} as id_paciente,

    concat(
      atendimento_tipo,
      " - ",
      atendimento_subtipo
    ) as ds_tipo_atendimento,

    -- Arredonda datetime ao minuto mais próximo
    datetime_trunc(
      datetime_add(datahora_entrada, interval 30 second),
      minute
    ) as data_entrada,
    datetime_trunc(
      datetime_add(datahora_saida, interval 30 second),
      minute
    ) as data_alta,

    profissional_nome as nm_medico_responsavel,
    profissional_cns as cns_medico_responsavel,
    profissional_cbo as cbo_medico_responsavel,

    encaminhamento as ds_motivo_alta,

    cid_principal as diagnostico_principal,
    cid_secundario as diagnostico_secundario,

    "sarah" as _prontuario_fonte
  from {{ ref("raw_prontuario_sarah__atendimento") }}
  where (paciente_nome is not null)
    and (paciente_data_nascimento is not null)
    and (cid_principal is not null)
),
paciente_existe as (
  select
    a.*
  from {{ ref("mart_projeto_ocis__paciente") }} as p
  inner join sarah_atendimento as a
    using (id_paciente)
)

select *
from paciente_existe
