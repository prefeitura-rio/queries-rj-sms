{{
    config(
        schema='intermediario_cdi',
        alias='atendimento_mandado_ata_vigente',
        materialized='table'
    )
}}

with base as (
    select *
    from {{ ref('raw_cdi__atendimento_mandado_atas_vigentes') }}
),

/*
    Tratamento dos campos numéricos:
    - Alguns valores usam ponto como separador de milhar (ex: "825.000", "90.000")
    - O campo 'utilizado' pode conter somas manuais (ex: "100 + 200", "900 + 900")
    - Campos vazios devem virar null

    Lógica para distinguir separador de milhar vs decimal:
    - Se o ponto é seguido de exatamente 3 dígitos no final → separador de milhar → remover
    - Caso contrário, manter (não esperado nesse dataset, mas seguro)
*/

tratado as (
    select
        -- === Identificadores ===
        processo_de_compra,
        num_item,
        codigo_item,
        num_pe,
        num_ata,

        -- === Descritivos ===
        objeto,
        empresa_vencedora,
        observacao,

        -- === Campos numéricos tratados ===

        -- qtd_ata: remover separador de milhar (ponto seguido de 3 dígitos)
        safe_cast(
            regexp_replace(
                nullif(trim(qtd_ata), ''),
                r'\.(\d{3})\b',
                r'\1'
            ) as numeric
        ) as qtd_ata,

        -- valor_unitario: mesmo tratamento de milhar
        safe_cast(
            regexp_replace(
                nullif(trim(valor_unitario), ''),
                r'\.(\d{3})\b',
                r'\1'
            ) as numeric
        ) as valor_unitario,

        -- pedidos: mesmo tratamento
        safe_cast(
            regexp_replace(
                nullif(trim(pedidos), ''),
                r'\.(\d{3})\b',
                r'\1'
            ) as numeric
        ) as pedidos,

        -- utilizado: tratar somas manuais ("100 + 200" → avaliar soma)
        -- Primeiro resolve as somas, depois converte
        (
            select sum(
                safe_cast(
                    regexp_replace(
                        trim(part),
                        r'\.(\d{3})\b',
                        r'\1'
                    ) as numeric
                )
            )
            from unnest(
                split(
                    nullif(trim(utilizado), ''),
                    '+'
                )
            ) as part
        ) as utilizado,

        -- saldo: mesmo tratamento de milhar
        safe_cast(
            regexp_replace(
                nullif(trim(saldo), ''),
                r'\.(\d{3})\b',
                r'\1'
            ) as numeric
        ) as saldo,

        -- === Datas de vigência ===
        data_inicio,
        data_fim,

        -- === Campos calculados para dashboards ===

        -- Ata vigente: data atual entre início e fim
        case
            when data_inicio is not null
                and data_fim is not null
                and current_date() between data_inicio and data_fim
            then true
            else false
        end as is_vigente,

        -- Dias até vencimento (negativo = já vencida)
        case
            when data_fim is not null
            then date_diff(data_fim, current_date(), day)
        end as dias_ate_vencimento,

        -- Flag: vence em até 30 dias
        case
            when data_fim is not null
                and date_diff(data_fim, current_date(), day) between 0 and 30
            then true
            else false
        end as is_vencimento_iminente

    from base
),

-- Calcular métricas de consumo com os campos já tratados
final as (
    select
        *,

        -- Percentual de consumo: utilizado / qtd_ata
        case
            when qtd_ata is not null
                and qtd_ata > 0
                and utilizado is not null
            then round(safe_divide(utilizado, qtd_ata) * 100, 2)
        end as percentual_consumo,

        -- Saldo crítico: saldo abaixo de 30% da qtd_ata
        case
            when qtd_ata is not null
                and qtd_ata > 0
                and saldo is not null
                and safe_divide(saldo, qtd_ata) < 0.3
            then true
            else false
        end as is_saldo_critico,

        -- Classificação do nível de consumo
        case
            when qtd_ata is null or qtd_ata = 0 then 'SEM DADOS'
            when utilizado is null or utilizado = 0 then 'SEM CONSUMO'
            when safe_divide(utilizado, qtd_ata) < 0.5 then 'CONSUMO BAIXO'
            when safe_divide(utilizado, qtd_ata) < 0.7 then 'CONSUMO MODERADO'
            when safe_divide(utilizado, qtd_ata) < 0.9 then 'CONSUMO ALTO'
            else 'CONSUMO CRITICO'
        end as nivel_consumo

    from tratado
)

select * from final