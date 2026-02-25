{{
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'atendimento_mandado_atas_vigentes'
    )
}}

with base as (

    select
        processo_de_compra,
        num_ata,
        objeto,
        empresa_vencedora,
        qtd_ata,
        valor_unitario,
        utilizado,
        saldo,
        data_inicio,
        data_fim,
        is_vigente,
        num_item,
        codigo_item,

        -- Flag: tem datas preenchidas?
        (data_inicio is not null and data_fim is not null) as tem_datas,

        -- dias até vencimento (só se tiver data_fim)
        case
            when data_fim is not null
            then date_diff(data_fim, current_date(), day)
        end as dias_ate_vencimento,

        -- percentual de consumo
        round(safe_divide(utilizado, qtd_ata) * 100, 2) as percentual_consumo,

        -- saldo crítico
        case
            when qtd_ata is not null and qtd_ata > 0
            then safe_divide(saldo, qtd_ata) < 0.3
            else false
        end as is_saldo_critico

    from {{ ref('int_cdi_atendimento_mandado_atas_vigentes') }}


),

final as (

    select
        processo_de_compra,
        num_ata,
        objeto,
        empresa_vencedora,
        qtd_ata,
        valor_unitario,
        utilizado,
        saldo,
        data_inicio,
        data_fim,
        tem_datas,
        dias_ate_vencimento,
        percentual_consumo,
        is_saldo_critico,
        is_vigente,

        -- Situação de vencimento com tratamento para sem data
        case
            when not tem_datas
                then 'Sem data de vigência'
            when dias_ate_vencimento < 0
                then concat('Vencida há ', abs(dias_ate_vencimento), ' dias')
            when dias_ate_vencimento = 0
                then 'Vence hoje'
            when dias_ate_vencimento = 1
                then 'Vence amanhã'
            else concat('Vence em ', dias_ate_vencimento, ' dias')
        end as situacao_vencimento,

        -- Vigência tratada: sem data = considerar como indefinida
        case
            when not tem_datas then 'INDEFINIDA'
            when is_vigente then 'VIGENTE'
            else 'VENCIDA'
        end as status_vigencia,

        -- Alerta de vencimento (false se não tem data)
        coalesce(dias_ate_vencimento between 0 and 30, false) as is_vencimento_iminente

    from base

)

select *
from final
