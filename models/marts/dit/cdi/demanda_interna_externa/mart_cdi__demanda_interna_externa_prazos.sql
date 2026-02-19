{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'demanda_interna_externa_prazos'
    ) 
}}

with base as (

    select *
    from {{ ref('int_cdi__demanda_interna_externa') }}

),

calculado as (

    select 
        processorio_sei,
        subsecretaria___setor,
        data_de_entrada,
        case
            when manifestacao is null or trim(manifestacao) = ''
                then 'Sem Manifestação'
            else manifestacao
        end as manifestacao,

        vencimento_1,
        -- Status do prazo - LÓGICA SIMPLES
        case
            -- Sem prazo
            when vencimento_1 is null
                then 'Sem Prazo Definido'
            
            -- Arquivadas: verifica data de conclusão vs vencimento
            when status = 'Arquivado' 
             and data_da_ultima_atualizacao is not null
                then
                    case
                        when data_da_ultima_atualizacao <= vencimento_1
                            then 'Dentro do Prazo'
                        else 'Fora do Prazo'
                    end
            
            -- Em andamento: verifica data atual vs vencimento
            when current_date() <= vencimento_1
                then 'Dentro do Prazo'
            
            else 'Fora do Prazo'
            
        end as status_prazo,
        data_da_ultima_atualizacao,
        orgao_demandante,
        tipo_de_demanda,
        status,
        case
            when status in (
                'Arquivado',
                'Concluído - Devolvido ao Setor de Origem'
            )
            then date_diff(
                data_da_ultima_atualizacao,
                data_de_entrada,
                day
            )
        end as tempo_atendimento,

        case
            when status not in (
                'Arquivado',
                'Concluído - Devolvido ao Setor de Origem'
            )
            and vencimento_1 is not null
            then date_diff(
                vencimento_1,
                current_date(),
                day
            )
        end as dias_para_vencer

    from base
)

select
    *,
    -- Situação descritiva do vencimento
    case
        when vencimento_1 is null
            then 'Sem prazo definido'
        
        when status = 'Arquivado'
            then "Arquivado"
        
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
        
        else 'Sem prazo definido'
    end as situacao_vencimento,

from calculado
