{{
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'atendimento_mandado_prazos_a_vencer'
    )
}}

with base as (

    select
        processo,
        data_entrada,
        prazo_limite,
        data_de_saida,
        sem_deferimento_ou_extinto,

        -- Filtros presentes no atendimento_mandado_filtros
        tipo_de_documento,
        tipo_de_solicitacao,
        pacientes_novos,
        direcionamento_interno,
        cap,
        sexo,
        estadual_federal,
        advogado_ou_defensoria,
        multas,
        responsavel_pela_saida_juridico,
        orgao_de_origem,

        -- Saldo de dias em relação ao vencimento
        date_diff(
            prazo_limite,
            current_date(),
            day
        ) as dias_para_vencer

    from {{ ref('int_cdi__atendimento_mandado') }}

    where
        -- Coluna "J": excluir registros com sem_deferimento_ou_extinto preenchido
        sem_deferimento_ou_extinto not in ('SEM DEFERIMENTO', 'EXTINTO')


),

final as (

    select
        processo,
        cap,
        prazo_limite as data_vencimento_prazo,
        dias_para_vencer,

        case
            when dias_para_vencer < -1
                then concat('Vencido há ', abs(dias_para_vencer), ' dias')
            when dias_para_vencer = -1
                then 'Venceu ontem'
            when dias_para_vencer = 0
                then 'Vence hoje'
            when dias_para_vencer = 1
                then 'Vence amanhã'
            when dias_para_vencer > 1
                then concat('Vence em ', dias_para_vencer, ' dias')
        end as situacao_vencimento,

        -- Filtros
        data_entrada,
        tipo_de_documento,
        tipo_de_solicitacao,
        pacientes_novos,
        direcionamento_interno,
        sexo,
        estadual_federal,
        advogado_ou_defensoria,
        multas,
        responsavel_pela_saida_juridico,
        orgao_de_origem

    from base

)

select *
from final
order by dias_para_vencer asc
