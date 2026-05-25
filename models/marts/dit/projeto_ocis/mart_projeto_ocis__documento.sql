{{
  config(
    alias="documento",
    materialized="table",
    schema="projeto_ocis",
    meta={"owner": "avellar"}
  )
}}


with source as (
  select *
  from {{ source("brutos_prontuario_sarah_api_staging", "atendimento_continuo") }}
  where source_id != "string"
  qualify row_number() over (partition by source_id order by datalake_loaded_at desc) = 1
),
documentos as (
  select
    json_value(data, "$.paciente.nome") as paciente_nome,
    json_value(data, "$.paciente.data_nascimento") as paciente_data_nascimento,

    json_value(doc, "$.profissional.cns") as profissional_cns,
    json_value(doc, "$.profissional.cbo") as profissional_cbo,

    json_value(doc, "$.numero") as documento_numero,
    json_value(doc, "$.datahora") as documento_datahora,
    json_value(doc, "$.tipo") as documento_tipo,

    json_query(doc, "$.dados") as documento_dados
  from source,
  unnest(json_extract_array(data, "$.documentos")) as doc
),

tratado as (
  select
    {{
      dbt_utils.generate_surrogate_key([
        "upper(regexp_replace(normalize(paciente_nome, NFD), r'[^\p{Letter}]', ''))",
        "paciente_data_nascimento"
      ])
    }} as id_paciente,

    
    {{
      dbt_utils.generate_surrogate_key([
        "documento_numero", "documento_tipo"
      ])
    }} as id_registro,
    date(datetime(documento_datahora)) as data_registro,

    case
      when documento_tipo in (
        "ATENDIMENTO MÉDICO", "CONTRAREFERÊNCIA"
      )
        then 6
      when documento_tipo = "EVOLUÇÃO MULTIPROFISSIONAL"
        then 8
      when starts_with(documento_tipo, "RECEITUÁRIO")
        then 15
      else 21
    end as id_tipo_registro,

    case
      when documento_tipo in (
        "ATENDIMENTO MÉDICO", "CONTRAREFERÊNCIA"
      )
        then "Evolução Médica"
      when documento_tipo = "EVOLUÇÃO MULTIPROFISSIONAL"
        then "Evolução Multidisciplinar"
      when starts_with(documento_tipo, "RECEITUÁRIO")
        then "Receita"
      else "Outros"
    end as tipo_registro,

    documento_tipo as titulo_documento,
    documento_dados as descricao_conteudo,

    {{ process_null("profissional_cns") }} as cns_profissional_responsavel,
    profissional_cbo as cbo_profissional_responsavel,

    "sarah" as _prontuario_fonte
  from documentos
  where (paciente_nome is not null)
    and (paciente_data_nascimento is not null)
),

paciente_existe as (
  select
    t.*
  from {{ ref("mart_projeto_ocis__paciente") }} as p
  inner join tratado as t
    using (id_paciente)
)

select *
from paciente_existe
