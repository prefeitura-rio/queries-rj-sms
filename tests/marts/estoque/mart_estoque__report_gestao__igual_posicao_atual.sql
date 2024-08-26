-- Teste se a posição calculada e CMD batem com a posição entregue no BI de farmácia
with
    medicamentos as (
        select id_material, nome from {{ ref("mart_estoque__report_gestao") }}
    ),

    posicao as (
        select
            id_material,
            id_cnes,
            sum(material_quantidade) as material_quantidade,
            avg(material_consumo_medio) as material_consumo_medio
        from {{ ref("mart_estoque__posicao_atual") }}
        where
            lote_validade_dentro_indicador = "sim"
            and estabelecimento_tipo_sms_agrupado in ("APS", "TPC")
        group by 1, 2
    ),

    posicao_consolidada as (
        select
            id_material,
            sum(material_quantidade) as qtd_pos,
            sum(material_consumo_medio) as cmd_pos
        from posicao
        group by 1
    ),

    relatorio as (
        select id_material, (qtd_aps + qtd_tpc) as qtd_relatorio, cmd
        from {{ ref("mart_estoque__report_gestao") }}
    ),

    joined as (
        select
            p.id_material,
            p.qtd_pos,
            r.qtd_relatorio,
            cmd_pos,
            r.cmd
        from posicao_consolidada as p
        left join relatorio as r using (id_material)
    )

select *, abs(qtd_pos - qtd_relatorio) as dif_qtd, abs(cmd_pos - cmd) as dif_cmd
from joined
where abs(qtd_pos - qtd_relatorio) > 0 or abs(cmd_pos - cmd) > 0.01
