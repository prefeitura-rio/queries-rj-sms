{{
    config(
        schema="brutos_prescricao",
        alias="materiais",
        materialized="table",
        tags=["raw", "pcsm", "material"],
        description="Itens e materiais padronizados disponíveis para uso em todos os hospitais e unidades de saúde da rede municipal do Rio de Janeiro."
    )
}}

select
    safe_cast(codmat as string) as id_material,                  -- Código único de identificação do material ou medicamento no catálogo
    safe_cast(grupo as string) as grupo_principal,               -- Código do grupo ao qual o material pertence
    safe_cast(subgr as string) as subgrupo_principal,            -- Código do subgrupo dentro do grupo principal do material
    safe_cast(class as string) as classe_subgrupo,               -- Código de classificação do material
    safe_cast(unida as string) as unidade_medida,                -- Unidade de medida do material
    safe_cast(csiaf as string) as custo_aquisicao,               -- Código de classificação SIAF
    safe_cast(ssiaf as string) as custo_substituicao,            -- Sub-código SIAF
    safe_cast(pecon as string) as peso_unidade,                  -- Indicador de controle de estoque ou tipo de consumo do material
    case trim(safe_cast(pecon as string))
        when 'P' then 'Permanente'
        when 'C' then 'Consumo'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_consumo,                               -- Descrição do tipo de consumo do material
    safe_cast(descr as string) as descricao_detalhada,           -- Descrição detalhada do material ou medicamento
    safe_cast( {{process_null('status')}} as string) as status_atual,                 -- Status do material no catálogo
    case trim(upper(safe_cast(status as string)))
        when 'S' then 'Ativo'
        when 'I' then 'Inativo'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_status,                                    -- Descrição do status do material
    safe_cast( {{process_null('tipo')}} as string) as tipo_material,                  -- Tipo de material
    case trim(safe_cast(tipo as string))
        when 'M' then 'Medicamento'
        when 'I' then 'Insumo'
        when '' then 'Não informado'
        when null then 'Não informado'
        else 'Não classificado'
    end as descricao_tipo_material,                               -- Descrição do tipo de material
    _airbyte_extracted_at as loaded_at,
    current_timestamp() as transformed_at
from {{ source('brutos_prescricao_staging', 'fa_catalogmat') }}