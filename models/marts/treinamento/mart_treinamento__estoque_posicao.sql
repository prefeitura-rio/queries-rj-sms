{{
    config(
        alias="estoque_posicao_atual",
        schema="projeto_treinamento_escritorio",
    )
}}


with source as (select * from `rj-sms`.`projeto_estoque`.`estoque_posicao_atual`)

select * from source
where estabelecimento_area_programatica = "31"
and lote_dias_para_vencer > -10
-- order by lote_dias_para_vencer
