{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'demanda_interna_externa_derivados_prazos'
    ) 
}}

with base as (
    select *
    from {{ ref('int_cdi__demanda_interna_externa_derivados') }}
),

base_pai as (
    select distinct
        processorio_sei,
        orgao_demandante,
        coalesce(manifestacao, 'Sem informação') as manifestacao,
        coalesce(tipo_de_demanda, 'Sem informação') as tipo_de_demanda,
        unidade_ap
    from {{ ref('int_cdi__demanda_interna_externa') }}
),

calculado as (
    select 
        b.processorio,
        b.derivado_do_processorio,
        b.subsecretaria___setor,
        b.vencimento,
        b.data_de_emissao as data_de_entrada,
        b.breve_descricao_da_solicitacao,
        b.data_da_ultima_atualizacao,
        b.status,
        b.observacao,

        -- Campos da base pai para filtros
        p.orgao_demandante,
        p.manifestacao,
        p.tipo_de_demanda,
        p.unidade_ap,

        case
            when b.status in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
            and b.data_da_ultima_atualizacao is not null
            and b.data_de_emissao is not null
            then date_diff(
                b.data_da_ultima_atualizacao,
                b.data_de_emissao,
                day
            )
        end as tempo_atendimento,

        case
            when b.vencimento is null
                then 'Sem Prazo Definido'
            
            when b.status in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
             and b.data_da_ultima_atualizacao is not null
                then
                    case
                        when b.data_da_ultima_atualizacao <= b.vencimento
                            then 'Dentro do Prazo'
                        else 'Fora do Prazo'
                    end
            
            when b.status in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
             and b.data_da_ultima_atualizacao is null
                then 'Sem Data de Conclusão'
            
            when current_date() <= b.vencimento
                then 'Dentro do Prazo'
            
            else 'Fora do Prazo'
            
        end as status_prazo,

        case
            when b.status not in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
            and b.vencimento is not null
            then date_diff(
                b.vencimento,
                current_date(),
                day
            )
        end as dias_para_vencer

    from base b
    left join base_pai p
        on b.processorio = p.processorio_sei
)

select
    *,
    
    case
        when status in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
            then 'Concluído'

        when dias_para_vencer is null
            then 'Sem Prazo Definido'

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

    case
        when status not in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
         and dias_para_vencer is not null 
         and dias_para_vencer between 0 and 7
            then true
        else false
    end as vencimento_iminente

from calculado