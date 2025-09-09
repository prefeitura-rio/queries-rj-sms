{{
    config(
        alias="protocolos",
        materialized="table"
    )
}}

with
    -- ------------------------------------------------------------
    -- Eventos
    -- ------------------------------------------------------------
    eventos as (
        select * from {{ ref("mart_iplanrio_pic__eventos") }}
    ),
    eventos_criancas as (
        select * from eventos
        where tipo_publico = 'Infancia'
    ),

    -- ------------------------------------------------------------
    -- Publico Alvo
    -- ------------------------------------------------------------
    publico_alvo as (
        select * from {{ ref("mart_iplanrio_pic__publico_alvo") }}
    ),
    criancas as (
        select * from publico_alvo
        where tipo_publico = 'Infancia'
    ),

    -- ------------------------------------------------------------
    -- Protocolo 1: 3 dose da Pentavalente, aos 12 meses de idade
    -- ------------------------------------------------------------
    -- Avaliação de Aplicabilidade: Criancas com data de nascimento maior que 12 meses
    p1_crianca_com_aplicabilidade as (
        select
            *,
            data_referencia > DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) as aplicavel
        from criancas
    ),

    -- Avaliação de Eventos: Vacinações Pentavalente
    p1_eventos_aplicaveis as (
        select
            publico_alvo.cpf, 
            publico_alvo.aplicavel,
            coalesce(array_agg(distinct eventos_criancas.tipo_evento), array[]) as eventos,
            max(eventos_criancas.distancia_dias) as ultima_atualizacao
        from p1_crianca_com_aplicabilidade publico_alvo
            left join eventos_criancas using (cpf)
        where eventos_criancas.tipo_evento in (
            'Vacina - DTP/HB/Hib - 1ª Dose',
            'Vacina - DTP/HB/Hib - 2ª Dose',
            'Vacina - DTP/HB/Hib - 3ª Dose'
        )
        group by 1, 2
    ),
    p1_avaliacao_eventos as (
        SELECT
            cpf,
            case
                when aplicavel = false then 'Não Aplicável'
                when array_length(eventos) = 3 and ultima_atualizacao < 365 then 'Aprovado por Mérito'
                when array_length(eventos) = 3 then 'Aprovado por Correção'
                else 'Reprovado'
            end as status
        FROM p1_eventos_aplicaveis
    ),

    -- ------------------------------------------------------------
    -- Junção dos resultados
    -- ------------------------------------------------------------
    resultados_protocolo as (
        select
            p1_avaliacao_eventos.cpf,
            p1_avaliacao_eventos.status as protocolo_pentavalente
        from p1_avaliacao_eventos
    )
select * 
from resultados_protocolo