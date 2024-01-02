{{
    config(
        alias="estoque_movimento",
        schema="projeto_treinamento_escritorio",
    )
}}


with source as (select * from `rj-sms`.`projeto_estoque`.`estoque_movimento`)

select *
from source
where
    estabelecimento_area_programatica = "31"
    and data_particao between "2023-12-03" and "2023-12-09"
