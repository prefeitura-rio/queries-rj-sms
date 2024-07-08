{{
    config(
        alias="estoque_movimento",
        schema="brutos_prontuario_vitai",
        labels={"contains_pii": "no"},
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month",
        },
        enabled=false
    )
}}

with source as (
      select * from {{ source('brutos_prontuario_vitai_staging', 'estoque_movimento') }}
),
renamed as (
    select
        {{ adapter.quote("produtoId") }} as id_material_vitai,
        {{ adapter.quote("estabelecimentoId") }} id_estabelecimento_vitai,
        {{ adapter.quote("cnes") }} as id_cnes,
        {{ adapter.quote("sigla") }} as estabelecimento_sigla,
        {{ adapter.quote("produtoCodigo") }} as id_material,
        {{ adapter.quote("descricao") }} as material_descricao,
        {{ adapter.quote("grupo") }} as material_grupo,
        {{ adapter.quote("subGrupo") }} as material_subgrupo,
        {{ adapter.quote("categoria") }} as material_categoria,
        {{ adapter.quote("apresentacao") }} as material_unidade,
        {{ adapter.quote("tipoMovimento") }} as estoque_movimento_tipo,
        {{ adapter.quote("justificativaMovimentacao") }} as estoque_movimento_justificativa,
        {{ adapter.quote("secaoOrigem") }} as estoque_secao_origem,
        {{ adapter.quote("secaoDestino") }} as estoque_secao_destino,
        {{ adapter.quote("pacienteCpf") }} as dispensacao_paciente_cpf,
        {{ adapter.quote("pacienteCns") }} as dispensacao_paciente_cns,
        {{ adapter.quote("prescritorCpf") }} as dispensacao_prescritor_cpf,
        {{ adapter.quote("quantidade") }} as material_quantidade,
        {{ adapter.quote("valor") }} as material_valor_total,
        {{ adapter.quote("dataMovimentacao") }} as estoque_movimento_data_hora,
        {{ adapter.quote("dataHora") }} as data_atualizacao,
        {{ adapter.quote("_data_carga") }} as data_carga,
        {{ adapter.quote("ano_particao") }},
        {{ adapter.quote("mes_particao") }},
        {{ adapter.quote("data_particao") }}

    from source
)


select
    -- Primary Key
    -- Foreign Keys
    safe_cast(id_cnes as string) as id_cnes,
    safe_cast(
        regexp_replace(id_material, r'[^a-zA-Z0-9]', '') as string
    ) as id_material,
    -- Common fields
    material_descricao,
    material_unidade,
    estoque_secao_origem,
    estoque_secao_destino,
    estoque_movimento_tipo,
    estoque_movimento_justificativa,
    DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',  estoque_movimento_data_hora), 'UTC-03:00') as  estoque_movimento_data_hora,
    safe_cast(
        DATETIME(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez',  estoque_movimento_data_hora), 'UTC-03:00') as date
    ) as estoque_movimento_data,
    dispensacao_paciente_cpf,
    dispensacao_paciente_cns,
    dispensacao_prescritor_cpf,
    safe_cast(material_quantidade as float64) as material_quantidade,
    safe_cast(material_valor_total as float64) as material_valor_total,

    -- Metadata
    safe_cast(data_atualizacao as datetime) as data_atualizacao,
    safe_cast(data_particao as date) as data_particao,
    safe_cast(data_carga as datetime) as data_carga,

from renamed
where
    (id_cnes = "2270242" and safe_cast(data_particao as date) >= "2023-07-01")  -- Barata Ribeiro estava implantação até 2023-06-30
    or (id_cnes <> "2270242" and id_cnes <> "2970619") -- demais unidades exceto Centro Carioca dos Olhos

{% if is_incremental() %}

    safe_cast(data_particao as date) > (select max(data_particao) from {{ this }})

{% endif %}

