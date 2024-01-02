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
select * from renamed
