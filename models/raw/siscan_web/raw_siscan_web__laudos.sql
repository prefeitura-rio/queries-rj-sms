-- noqa: disable=LT08
{{
  config(
    enabled=true,
    schema="brutos_siscan_web",
    alias="laudos_mamografia",
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='protocolo_id',
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "month",
    },
cluster_by = ['protocolo_id', 'unidade_prestadora_id_cnes', 'unidade_solicitante_id_cnes', 'mamografia_tipo'],
on_schema_change = 'sync_all_columns'
)
}}

{% set src = source('brutos_siscan_web_staging', 'laudos') %} -- tabela antiga de laudos de mamografia
{% set src_new = source('brutos_siscan_web_staging', 'laudos_mamografia') %} -- nova tabela com os laudos de mamografia
{% set cols = adapter.get_columns_in_relation(src) %}
{% set last_partition = get_last_partition_date( this ) %}

with
-- garantindo unicidade do n_protocolo,
-- obtendo o registro mais atual,
-- e pegando so a particao mais atual em staging.
-- obs: na tabela em staging a data_particao é construida a partir de data_extracao,
-- porem neste modelo a data_particao é definida posteriormente como a data_realizacao
    source as (
        {% if not is_incremental() %}
          select *
          from {{ src }}

          union all

          select *
          from {{ src_new }}

          qualify row_number() over (
              partition by n_protocolo
              order by data_particao desc nulls last
          ) = 1
        {% else %}

        select *,
          -- gambiarra máxima (nao ir pra producao!!!)
          -- o scrapper nao está mais pegando os seguintes campos por algum motivo:
          "" as responsavel_resultado,
          "" as cns_resultado,
          "" as conselho,
          "" as data_liberacao_resultado 

        from {{ src_new }}
        where data_particao >= '{{ last_partition }}'

        qualify row_number() over (
            partition by n_protocolo
            order by data_particao desc nulls last
        ) = 1

        {% endif %}
    ),

-- aplicando macro process_null em todas as colunas do tipo string
    source_norm as (
        select
            {%- for c in cols %}
            {%- set dtype = (c.data_type) -%}
            {%- if dtype in ['STRING'] -%}
            {{ process_null('s.' ~ adapter.quote(c.name)) }} as {{ adapter.quote(c.name) }}
            {%- else -%}
            s.{{ adapter.quote(c.name) }}
            {%- endif -%}
            {%- if not loop.last %}, {% endif -%}
            {%- endfor %}
        from source as s
    )

-- OBS: Os dados são extraídos da fonte por data_liberacao_resultado
--          (Roda diariamente para d-1, e uma vez por mês para os 30 últimos dias)
--          Exames com data_realizacao diversos podem sair em data_liberacao_resultado diversos.
select
    -- id do registro
    n_protocolo as protocolo_id, -- está sendo utilizado como pk. é pk mesmo? quantos digitos deve ter?
    n_exame as exame_id, -- checar se é único e quantidade de chars para padronizar
    n_prontuario as prontuario_id, -- parece que está 100% (ou quase) vazio. checar!

    -- id do paciente
    lpad(safe_cast(safe_cast(paciente_cartao_sus as int) as string), 15, "0") as paciente_cns,
    {{ clean_name_string("paciente_nome") }} as paciente_nome,
    {{ clean_name_string("paciente_mae") }} as paciente_nome_mae,
    parse_date('%d/%m/%Y', paciente_dt_nasc) as paciente_data_nasc,
    safe_cast(paciente_idade as int) as paciente_idade,
    paciente_sexo,
    paciente_telefone,

    -- endereco paciente
    paciente_uf,
    {{ clean_name_string("paciente_municipio")}} as paciente_municipio,
    {{ clean_name_string("paciente_bairro") }} as paciente_bairro,
    paciente_cep,
    {{ clean_name_string("paciente_logradouro") }} as paciente_logradouro,
    paciente_endereco_complemento,
    paciente_endereco_numero,

    -- datas
    parse_date('%Y-%m-%d', data_solicitacao) as data_solicitacao,
    parse_date('%d/%m/%Y', data_realizacao) as data_realizacao,
    parse_date('%d/%m/%Y', data_liberacao_resultado) as data_liberacao_resultado,
    parse_date('%d/%m/%Y',
        nullif(
            trim(split(data_ultima_menstruacao, ':') [safe_offset(1)]),
            'Não lembra'
        )
    ) as data_ultima_menstruacao,

    -- resultados exame - mama esquerda
    tipo_mama_esquerda as mama_esquerda_tipo,
    mama_esquerda_pele,
    linfonodos_axiliares_esquerda as mama_esquerda_linfonodos_axilares, -- incompleto, falta dilatacao ductal ex:121528446
    split(achados_benignos_esquerda, ', ') as mama_esquerda_achados_benignos,
    -- SCRAPPER BUG: FALTANDO `achado_exame_esquerda`
    trim(split(classif_radiologica_esquerda, ':') [safe_offset(1)]) as mama_esquerda_classif_radiologica,

    -- resultados exame - mama direita
    tipo_mama_direita as mama_direita_tipo,
    mama_direita_pele,
    linfonodos_axiliares_direita as mama_direita_linfonodos_axilares, -- incompleto, falta dilatacao ductal ex:121528446
    split(achados_benignos_direita, ', ') as mama_direita_achados_benignos,
    achado_exame_direita as mama_direita_achado_exame,
    trim(split(classif_radiologica_direita, ':') [safe_offset(1)]) as mama_direita_classif_radiologica,

    -- infos exame geral
    mamografia_tipo,
    mamografia_rastreamento_tipo,
    microcalcificacoes as mamografia_microcalcificacoes,
    safe_cast(numero_filmes as int) as mamografia_numero_filmes,
    recomendacoes as mamografia_recomendacoes,
    observacoes_gerais as mamografia_observacoes_gerais,
    -- achado_exame_clinico as mamografia_achado_exame_clinico, -- SCRAPPER BUG: só vem ("Achados no exame clínico:"", '')
    -- texto_mamas_labels as mamografia_labels, -- SCRAPPER BUG: só vem ("Ministério da Saúde")

    -- unidade solicitante (? CONFIRMAR)
    unidade_uf as unidade_solicitante_uf, -- aparentemente tudo RJ?
    {{ clean_name_string("unidade_municipio") }} as unidade_solicitante_municipio,
    upper(trim(unidade_nome)) as unidade_solicitante_nome,
    lpad(safe_cast(safe_cast(unidade_cnes as int) as string), 7, "0") as unidade_solicitante_id_cnes,

    -- unidade que realizou o exame (? CONFIRMAR)
    prestador_uf as unidade_prestadora_uf, -- apenas RJ?
    {{ clean_name_string("prestador_municipio") }} as unidade_prestadora_municipio,
    upper(trim(prestador_nome)) as unidade_prestadora_nome,
    lpad(safe_cast(safe_cast(prestador_cnes as int) as string), 7, "0") as unidade_prestadora_id_cnes,
    lpad(safe_cast(safe_cast(prestador_cnpj as int) as string), 14, "0") as unidade_prestadora_cnpj,

    -- Profissional responsável pelo resultado (? CONFIRMAR)
    lpad(safe_cast(safe_cast(cns_resultado as int) as string), 15, "0") as profissional_responsavel_cns,
    trim(split(conselho, ' - ') [safe_offset(1)]) as profissional_responsavel_crm,
    {{ clean_name_string("responsavel_resultado") }} as profissional_responsavel_nome,

    -- metadados
    parse_timestamp('%Y-%m-%d %H:%M:%E6S', data_extracao) as data_extracao,
    parse_date('%d/%m/%Y', data_realizacao) as data_particao

from source_norm
