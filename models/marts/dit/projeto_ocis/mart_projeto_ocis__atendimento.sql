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
    to_hex(sha1(atendimento_numero)) as atendimento_id,
    {{
      dbt_utils.generate_surrogate_key([
        "upper(regexp_replace(normalize(paciente_nome, NFD), r'[^\p{Letter}]', ''))",
        "paciente_data_nascimento"
      ])
    }} as paciente_id,

    concat(
      atendimento_tipo,
      " - ",
      atendimento_subtipo
    ) as atendimento_tipo,

    -- Arredonda datetime ao minuto mais próximo
    datetime_trunc(
      datetime_add(datahora_entrada, interval 30 second),
      minute
    ) as datahora_entrada,
    datetime_trunc(
      datetime_add(datahora_saida, interval 30 second),
      minute
    ) as datahora_saida,

    if(
      profissional_cns is null,
      null,
      to_hex(sha1(profissional_cns))
    ) as profissional_id,
    profissional_cbo,

    encaminhamento,

    cid_principal,
    cid_secundario,

    "sarah" as prontuario
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
    using (paciente_id)
)

select *
from paciente_existe
