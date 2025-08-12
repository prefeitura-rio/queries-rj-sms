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
    cluster_by=['unidade_prestadora_id_cnes', 'unidade_solicitante_id_cnes', 'mamografia_tipo'],
    on_schema_change='sync_all_columns'
  )
}}

with
-- garantindo unicidade do n_protocolo e obtendo o registro mais atual
source as (
    select * 
    from {{source('brutos_siscan_web_staging', 'laudos')}}
    qualify row_number() over (
        partition by n_protocolo
        order by data_extracao desc nulls last
    ) = 1
)

-- OBS: Nos casos de TRIM + UPPER: Analisar distincts
-- OBS2: Os dados são extraídos da fonte por data_liberacao_resultado
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
    paciente_sexo, -- TRIM + UPPER?
    paciente_telefone,

    -- endereco paciente
    paciente_uf, -- TRIM + UPPER?
    paciente_municipio, -- TRIM + UPPER?
    {{ clean_name_string("paciente_bairro") }} as paciente_bairro,
    paciente_cep, -- está no formato 22783-117.. tratar? passar pra int?
    {{ clean_name_string("paciente_logradouro") }} as paciente_logradouro,
    paciente_endereco_complemento,
    paciente_endereco_numero,

    -- datas
    parse_date('%Y-%m-%d', data_solicitacao) as data_solicitacao,
    parse_date('%d/%m/%Y', data_realizacao) as data_realizacao,
    data_ultima_menstruacao, -- SCRAPPER BUG: pegando errado
    parse_date('%d/%m/%Y', data_liberacao_resultado) as data_liberacao_resultado,

    -- resultados exame - mama esquerda
    tipo_mama_esquerda as mama_esquerda_tipo, -- TRIM + UPPER?
    mama_esquerda_pele, -- TRIM + UPPER? 
    linfonodos_axiliares_esquerda as mama_esquerda_linfonodos_axilares, -- TRIM + UPPER?
    achados_benignos_esquerda as mama_esquerda_achados_benignos, -- TRIM + UPPER?
    -- SCRAPPER BUG: FALTANDO `achado_exame_esquerda`
    trim(split(classif_radiologica_esquerda, ':')[safe_offset(1)]) as mama_esquerda_classif_radiologica,

    -- resultados exame - mama direita
    tipo_mama_direita as mama_direita_tipo, -- TRIM + UPPER?
    mama_direita_pele, -- TRIM + UPPER?
    linfonodos_axiliares_direita as mama_direita_linfonodos_axilares, -- TRIM + UPPER?
    achados_benignos_direita as mama_direita_achados_benignos, -- TRIM + UPPER?
    achado_exame_direita as mama_direita_achado_exame,
    trim(split(classif_radiologica_direita, ':')[safe_offset(1)]) as mama_direita_classif_radiologica,

    -- infos exame geral
    mamografia_tipo, -- (diagnostica/rastreamento) - TRIM + UPPER?
    mamografia_rastreamento_tipo, -- (populacao alvo, risco elevado, em tratamento) TRIM + UPPER?
    microcalcificacoes as mamografia_microcalcificacoes,    
    safe_cast(numero_filmes as int) as mamografia_numero_filmes, 
    recomendacoes as mamografia_recomendacoes, -- TRIM + UPPER?
    observacoes_gerais as mamografia_observacoes_gerais,
    achado_exame_clinico as mamografia_achado_exame_clinico, -- SCRAPPER BUG: aparentemente vindo todo vazio (checar)
    texto_mamas_labels as mamografia_labels, -- SCRAPPER BUG: aparentemente todos os registros com "Ministério da Saúde"

    -- unidade solicitante (? CONFIRMAR)
    unidade_uf as unidade_solicitante_uf, -- TRIM + UPPER?
    lpad(safe_cast(safe_cast(unidade_cnes as int) as string), 7, "0") as unidade_solicitante_id_cnes,
    unidade_nome as unidade_solicitante_nome, -- TRIM + UPPER?
    unidade_municipio as unidade_solicitante_municipio, -- TRIM + UPPER?

    -- unidade que realizou o exame (? CONFIRMAR)
    prestador_uf as unidade_prestadora_uf, -- TRIM + UPPER?
    prestador_municipio as unidade_prestadora_municipio, -- TRIM + UPPER?
    lpad(safe_cast(safe_cast(prestador_cnes as int) as string), 7, "0") as unidade_prestadora_id_cnes,
    prestador_nome as unidade_prestadora_nome, -- TRIM + UPPER?
    lpad(safe_cast(safe_cast(prestador_cnpj as int) as string), 14, "0") as unidade_prestadora_cnpj,

    -- Profissional responsável pelo resultado (? CONFIRMAR)
    {{ clean_name_string("responsavel_resultado") }} as profissional_responsavel_nome,
    lpad(safe_cast(safe_cast(cns_resultado as int) as string), 15, "0") as profissional_responsavel_cns,
    trim(split(conselho, ' - ')[safe_offset(1)]) as profissional_responsavel_crm,
    
    -- metadados
    parse_timestamp('%Y-%m-%d %H:%M:%E6S', data_extracao) as data_extracao,
    parse_date('%d/%m/%Y', data_realizacao) as data_particao

from source
{% if is_incremental() %}
    where 
        data_extracao > (select max(data_extracao) from {{ this }})
{% endif %}
