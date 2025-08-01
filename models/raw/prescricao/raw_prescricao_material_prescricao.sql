{{
    config(
        schema="brutos_prontuario_carioca_saude_mental_prescricao",
        alias="material_prescricao",
        materialized="table",
        tags=["raw", "pcsm", "material_prescricao"],
        description="Materiais específicos para prescrição médica em todas as unidades de saúde da rede municipal do Rio de Janeiro."
    )
}}

select
  safe_cast(codmat as string) as id_material,           -- Código do material ou medicamento (chave primária e estrangeira para fa_catalogmat)
  safe_cast(indtipoprescr as string) as tipo_prescricao,-- Indicador do tipo de prescrição associada ao material
  (
    select array_agg(
      case
        {% for tipo, descricao in {
            '1': 'Receituário comum',
            '2': 'Receituário Controlado',
            '3': 'Receituário com Imagens',
            '4': 'Receituário Excepcional'
          }.items() %}
        when t = '{{ tipo }}' then '{{ descricao }}'
        {% endfor %}
        else 'Desconhecido'
      end
    )
    from unnest(split(safe_cast(indtipoprescr as string), ',')) as t
  ) as descricao_tipo_prescricao,                        -- Descrição do tipo de prescrição associada ao material
  _airbyte_extracted_at as loaded_at,
  current_timestamp() as transformed_at
from {{ source('brutos_prontuario_carioca_saude_mental_prescricao_staging', 'fa_catalogmat_prescr') }}
