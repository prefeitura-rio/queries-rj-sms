with material_sigma as (select * from {{ ref('material') }} ),

material_em_estoque as (select distinct id_cnes, id_material from {{ ref('estoque_posicao') }}),

material_fora_do_sigma 

