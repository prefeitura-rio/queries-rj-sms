{{
    config(
        alias="controle_presenca",
        materialized="table",
    )
}}


with
    presenca as (
        select *
        from {{ ref("int_seguir_em_frente__controle_presenca_drop_duplicates") }}
    ),

    bolsista as (select * from {{ ref("mart_seguir_em_frente__bolsista") }})

select
    p.cpf,
    b.nome,
    b.fase_atual,
    case
        when b.fase_atual = "1"
        then UPPER(b.fase_1_estabelecimento)
        when b.fase_atual = "2"
        then UPPER(b.fase_2_estabelecimento)
        else null
    end as estabelecimento,
    p.observacoes,
    p.anexos,
    p.criado_por,
    p.criado_em,
    p.registro_data,
    format_date("%Y-%m", p.registro_data) as registro_data_competencia,
    p.registro_valor,
    if(p.registro_valor = "falta", 0, 1) as registro_valor_numerico
from presenca as p
left join bolsista as b on p.cpf = b.cpf
