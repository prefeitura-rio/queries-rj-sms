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
        select
            *,
            date_diff(current_date(), data_referencia, day) as dias_de_nascimento
        from publico_alvo
        where tipo_publico = 'Infancia'
    ),

    -- ------------------------------------------------------------
    -- Protocolo 1: 3 dose da Pentavalente, aos 12 meses de idade
    -- ------------------------------------------------------------
    p1_eventos_aplicaveis as (
        select
            publico_alvo.cpf,
            publico_alvo.dias_de_nascimento,
            coalesce(array_agg(distinct eventos_criancas.tipo_evento), array[]) as eventos,
            max(eventos_criancas.distancia_dias) as ultima_atualizacao
        from criancas publico_alvo left join eventos_criancas on 
            (publico_alvo.cpf = eventos_criancas.cpf) and 
            (eventos_criancas.tipo_evento in (
                'Vacina - DTP/HB/Hib - 1ª Dose',
                'Vacina - DTP/HB/Hib - 2ª Dose',
                'Vacina - DTP/HB/Hib - 3ª Dose'
            ))
        group by 1,2
    ),
    p1_avaliacao_eventos as (
        SELECT
            cpf,

            case
                when array_length(eventos) = 3 and ultima_atualizacao < 365 then 'Aprovado por Mérito'
                when array_length(eventos) = 3 then 'Aprovado por Correção'
                when dias_de_nascimento < 365 then 'Atenção'
                else 'Reprovado'
            end as status
        FROM p1_eventos_aplicaveis
    ),

    -- ------------------------------------------------------------
    -- Protocolo 2: Calendário de Consultas Mínimas
    -- ------------------------------------------------------------
    p2_avaliacao_eventos as (
        select
            publico_alvo.cpf,

            -- Até 1 ano de idade
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 0, 30, 1) }} as teve_consultas_ate_1_mes,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 30, 90, 2) }} as teve_consultas_ate_3_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 90, 210, 2) }} as teve_consultas_ate_7_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 210, 365, 2) }} as teve_consultas_ate_12_meses,
            -- Entre 1 e 2 anos de idade: semestral
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 365, 540, 1) }} as teve_consultas_entre_12_18_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 540, 730, 1) }} as teve_consultas_entre_18_24_meses,
            -- Entre 2 e 6 anos de idade: anual
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 730, 1095, 1) }} as teve_consultas_entre_24_36_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 1095, 1460, 1) }} as teve_consultas_entre_36_42_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 1460, 1825, 1) }} as teve_consultas_entre_42_48_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 1825, 2190, 1) }} as teve_consultas_entre_48_54_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 2190, 2555, 1) }} as teve_consultas_entre_54_60_meses,
        from criancas publico_alvo
            left join eventos_criancas on (publico_alvo.cpf = eventos_criancas.cpf) and (eventos_criancas.tipo_evento in ('Consulta'))
        group by publico_alvo.cpf, publico_alvo.dias_de_nascimento
    ),

    -- ------------------------------------------------------------
    -- Protocolo 3: Calendário de Visitas Domiciliares Mínimas
    -- ------------------------------------------------------------
    -- 1 VD por semestre, entre 0 e 6 anos de idade
    p3_avaliacao_eventos as (
        select
            publico_alvo.cpf,
            -- Ano 1
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 0, 182, 1) }} as teve_vd_entre_0_6_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 182, 365, 1) }} as teve_vd_entre_6_12_meses,
            -- Ano 2
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 365, 540, 1) }} as teve_vd_entre_12_18_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 540, 730, 1) }} as teve_vd_entre_18_24_meses,
            -- Ano 3
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 730, 1095, 1) }} as teve_vd_entre_24_36_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 1095, 1460, 1) }} as teve_vd_entre_36_42_meses,
            -- Ano 4
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 1460, 1825, 1) }} as teve_vd_entre_42_48_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 1825, 2190, 1) }} as teve_vd_entre_48_54_meses,
            -- Ano 5
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 2190, 2555, 1) }} as teve_vd_entre_54_60_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 2555, 2920, 1) }} as teve_vd_entre_60_66_meses,
            -- Ano 6
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 2920, 3285, 1) }} as teve_vd_entre_66_72_meses,
            {{ avalia_protocolo_consultas_minimas('publico_alvo.dias_de_nascimento', 3285, 3650, 1) }} as teve_vd_entre_72_78_meses
        from criancas publico_alvo 
            left join eventos_criancas on (publico_alvo.cpf = eventos_criancas.cpf) and (eventos_criancas.tipo_evento in ('Visita Domiciliar'))
        group by publico_alvo.cpf, publico_alvo.dias_de_nascimento
    ),



    -- ------------------------------------------------------------
    -- Junção dos resultados
    -- ------------------------------------------------------------
    resultados_protocolo as (
        select
            publico_alvo.cpf,
            publico_alvo.data_referencia,
            publico_alvo.tipo_publico,
            struct(
                coalesce(p1_avaliacao_eventos.status, 'Não Aplicável') as pentavalente
            ) as protocolo_vacinacao,
            struct(
                coalesce(p2_avaliacao_eventos.teve_consultas_ate_1_mes, 'Não Aplicável') as ate_1_mes,
                coalesce(p2_avaliacao_eventos.teve_consultas_ate_3_meses, 'Não Aplicável') as ate_3_meses,
                coalesce(p2_avaliacao_eventos.teve_consultas_ate_7_meses, 'Não Aplicável') as ate_7_meses,
                coalesce(p2_avaliacao_eventos.teve_consultas_ate_12_meses, 'Não Aplicável') as ate_12_meses,
                coalesce(p2_avaliacao_eventos.teve_consultas_entre_12_18_meses, 'Não Aplicável') as entre_12_18_meses,
                coalesce(p2_avaliacao_eventos.teve_consultas_entre_18_24_meses, 'Não Aplicável') as entre_18_24_meses,
                coalesce(p2_avaliacao_eventos.teve_consultas_entre_24_36_meses, 'Não Aplicável') as entre_24_36_meses,
                coalesce(p2_avaliacao_eventos.teve_consultas_entre_36_42_meses, 'Não Aplicável') as entre_36_42_meses,
                coalesce(p2_avaliacao_eventos.teve_consultas_entre_42_48_meses, 'Não Aplicável') as entre_42_48_meses,
                coalesce(p2_avaliacao_eventos.teve_consultas_entre_48_54_meses, 'Não Aplicável') as entre_48_54_meses,
                coalesce(p2_avaliacao_eventos.teve_consultas_entre_54_60_meses, 'Não Aplicável') as entre_54_60_meses
            ) as protocolo_consultas_puericultura,
            struct(
                coalesce(p3_avaliacao_eventos.teve_vd_entre_0_6_meses, 'Não Aplicável') as entre_0_6_meses,
                coalesce(p3_avaliacao_eventos.teve_vd_entre_6_12_meses, 'Não Aplicável') as entre_6_12_meses,
                coalesce(p3_avaliacao_eventos.teve_vd_entre_12_18_meses, 'Não Aplicável') as entre_12_18_meses,
                coalesce(p3_avaliacao_eventos.teve_vd_entre_18_24_meses, 'Não Aplicável') as entre_18_24_meses,
                coalesce(p3_avaliacao_eventos.teve_vd_entre_24_36_meses, 'Não Aplicável') as entre_24_36_meses,
                coalesce(p3_avaliacao_eventos.teve_vd_entre_36_42_meses, 'Não Aplicável') as entre_36_42_meses,
                coalesce(p3_avaliacao_eventos.teve_vd_entre_42_48_meses, 'Não Aplicável') as entre_42_48_meses,
                coalesce(p3_avaliacao_eventos.teve_vd_entre_48_54_meses, 'Não Aplicável') as entre_48_54_meses,
                coalesce(p3_avaliacao_eventos.teve_vd_entre_54_60_meses, 'Não Aplicável') as entre_54_60_meses
            ) as protocolo_visitas_domiciliares_puericultura,
            current_date() as ultima_atualizacao
        from publico_alvo
            left join p1_avaliacao_eventos using (cpf)
            left join p2_avaliacao_eventos using (cpf)
            left join p3_avaliacao_eventos using (cpf)
    )
select * 
from resultados_protocolo