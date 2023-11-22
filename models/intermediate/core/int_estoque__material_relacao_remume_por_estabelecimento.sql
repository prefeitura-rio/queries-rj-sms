with
    source as (select * from {{ ref("raw_sheets__material_remume") }}),
    remume_aps as (select * from source where remume_grupo = "Atenção Básica"),
    estabelecimento_aps as (
        select *
        from {{ ref("dim_estabelecimento") }}
        where
            tipo_cnes = "CENTRO DE SAUDE/UNIDADE BASICA"  # TODO: confirmar filtro
            and prontuario_estoque_tem_dado = "sim"  -- só mostrar unidade que usam o modulo de estoque do prontuario
    ),
    final_aps as (
        select est.id_cnes, remume.id_material, remume.material_descricao_generica, est.prontuario_versao
        from estabelecimento_aps as est
        cross join remume_aps as remume
    )

select id_cnes, id_material, material_descricao_generica, prontuario_versao
from final_aps
