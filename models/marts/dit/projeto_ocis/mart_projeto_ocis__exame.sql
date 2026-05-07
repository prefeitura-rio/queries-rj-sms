{{
  config(
    alias="exame",
    materialized="table",
    schema="projeto_ocis",
    meta={"owner": "avellar"}
  )
}}

with medilab_exames as (
  select distinct
    {{
      dbt_utils.generate_surrogate_key([
        "upper(regexp_replace(normalize(paciente_nome, NFD), r'[^\p{Letter}]', ''))",
        "paciente_data_nascimento"
      ])
    }} as id_paciente,

    exame_codigo_sigtap as id_exame,
    exame_nome as nm_exame,
    exame_data as data_execucao,

    "medilab" as _prontuario_fonte
  from {{ ref("raw_medilab__exames") }}
  where (paciente_nome is not null)
    and (paciente_data_nascimento is not null)
),
paciente_existe as (
  select
    e.*
  from {{ ref("mart_projeto_ocis__paciente") }} as p
  inner join medilab_exames as e
    using (id_paciente)
)

select *
from paciente_existe
