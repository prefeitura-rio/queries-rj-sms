-- noqa: disable=LT08
{{
  config(
    enabled=true,
    schema="brutos_siscan_web",
    alias="laudos_histo_mama",
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='protocolo_id',
    partition_by={
      "field": "data_particao",
      "data_type": "date",
      "granularity": "month",
    },
    cluster_by = ['protocolo_id', 'unidade_prestadora_id_cnes','unidade_solicitante_id_cnes', 'procedimento_cirurgico'],
    on_schema_change = 'sync_all_columns'
)
}}

{% set cols = adapter.get_columns_in_relation(source('brutos_siscan_web_staging', 'laudos_histo_mama')) %}
{% set last_partition = get_last_partition_date( this ) %}

with
source as (
    select *
    from {{ source('brutos_siscan_web_staging', 'laudos_histo_mama') }}

    {% if is_incremental() %}
        where data_particao >= '{{ last_partition }}'
    {% endif %}

    qualify row_number() over (
        partition by n_protocolo
        order by data_particao desc nulls last
    ) = 1
),

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
),

transformed as (
    select
        -- id do registro
        n_protocolo as protocolo_id, 
        n_exame as exame_id, 
        n_prontuario as prontuario_id,

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

        -- ESPECÍFICOS HISTO MAMA
        lateralidade, -- qual mama foi examinada
        localizacao, -- qual regiao da mama foi examinada
        procedimento_cirurgico, -- tipo de biopsia realizada
        exame_macroscopico, -- avaliação a olho nu do material coletado
        microcalcificacoes_histo, -- pequenos depositos de calcio (pode ser sinal de alerta)
        lesao_neoplasico, -- se foi encontrado cancer
        lesao_benigno, -- se foi encontrada lesão que não é cancer
        registrado_apac, 
        split(multifocalidade, ': ') [safe_offset(1)] as multifocalidade, -- se ha mais de um foco tumoral na mesma regiao da mama
        split(multicentricidade, ': ') [safe_offset(1)] as multicentricidade, -- se ha tumores em regioes diferentes da mama
        grau_histologico, -- indica a agressividade do tumor, quanto maior o grau, mais agressivo
        split(invasao_vascular, ': ') [safe_offset(1)] as invasao_vascular, -- se o tumor invadiu os vasos sanguineos
        split(infiltracao_perineural, ': ') [safe_offset(1)] as infiltracao_perineural, -- se o tumor invadiu os nervos
        split(embolizacao_linfatica, ': ') [safe_offset(1)] as embolizacao_linfatica, -- se as celulas tumorais invadiram os vasos linfaticos
        margens_cirurgicas, -- se as margens da cirurgia estão livres de tumor ou se há necessidade de nova cirurgia
        receptor_estrogeno, -- se o tumor é sensível ao hormônio estrogeno, o que pode indicar opções de tratamento hormonal
        receptor_progesterona, -- se o tumor é sensível ao hormônio progesterona, o que pode indicar opções de tratamento hormonal
        estudos_imuno, -- resultados de testes imuno-histoquimicos que ajudam a caracterizar o tumor
        observacoes_gerais_histo, 

        -- unidade solicitante
        unidade_uf as unidade_solicitante_uf,
        {{ clean_name_string("unidade_municipio") }} as unidade_solicitante_municipio,
        upper(trim(unidade_nome)) as unidade_solicitante_nome,
        lpad(safe_cast(safe_cast(unidade_cnes as int) as string), 7, "0") as unidade_solicitante_id_cnes,

        -- unidade que realizou o exame 
        prestador_uf as unidade_prestadora_uf,
        {{ clean_name_string("prestador_municipio") }} as unidade_prestadora_municipio,
        upper(trim(prestador_nome)) as unidade_prestadora_nome,
        lpad(safe_cast(safe_cast(prestador_cnes as int) as string), 7, "0") as unidade_prestadora_id_cnes,
        lpad(safe_cast(safe_cast(prestador_cnpj as int) as string), 14, "0") as unidade_prestadora_cnpj,

        -- Profissional responsável pelo resultado
        lpad(safe_cast(safe_cast(profissional_responsavel_cns as int) as string), 15, "0") as profissional_responsavel_cns,
        trim(split(profissional_responsavel_crm, ' - ') [safe_offset(1)]) as profissional_responsavel_crm,
        {{ clean_name_string("profissional_responsavel_nome") }} as profissional_responsavel_nome,

        -- metadados
        parse_timestamp('%Y-%m-%d %H:%M:%E6S', data_extracao) as data_extracao,
        parse_date('%d/%m/%Y', data_realizacao) as data_particao

    from source_norm
)

select *
from transformed
