--- Gera a maior data com dados da posição de estoque até o primeiro dia do mês atual

with

    primeiro_dia_mes as (select date_trunc(current_date('America/Sao_Paulo'), month) as data),

    posicao as (
        select * from {{ ref("fct_estoque_posicao") }} where sistema_origem = 'vitacare'
    ),

    primeira_posicao_por_estabelecimento as (
        select id_cnes, max(data_particao) as data_particao
        from posicao
        where data_particao <= (select data from primeiro_dia_mes)
        group by id_cnes
    )
    
select *
from primeira_posicao_por_estabelecimento
