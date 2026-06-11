{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'controle_interno_externo_prazos_consolidado'
    ) 
}}

with regulares as (

    select
        processorio_sei,
        subsecretaria_setor as subsecretaria,
        data_de_entrada,
        vencimento_1 as data_vencimento,
        data_da_ultima_atualizacao,
        status,
        orgao_demandante,
        manifestacao,
        tipo_de_demanda,
        unidade_ap,
        observacao,
        cast(null as string) as derivado_do_processorio,
        cast(null as string) as breve_descricao_da_solicitacao,

        -- Status do prazo
        case
            when vencimento_1 is null then 'Sem Prazo Definido'
            when status = 'Arquivado' and data_da_ultima_atualizacao is not null
                then case
                    when data_da_ultima_atualizacao <= vencimento_1 then 'Dentro do Prazo'
                    else 'Fora do Prazo'
                end
            when current_date() <= vencimento_1 then 'Dentro do Prazo'
            else 'Fora do Prazo'
        end as status_prazo,

        -- Tempo de atendimento
        case
        when ifnull(trim(status), '') in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
            and data_da_ultima_atualizacao is not null
            and data_de_entrada is not null
        then date_diff(data_da_ultima_atualizacao, data_de_entrada, day)
        else null
        end as tempo_atendimento,

        -- Dias para vencer
        case
            when status not in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
             and vencimento_1 is not null
                then date_diff(vencimento_1, current_date(), day)
            else null
        end as dias_para_vencer,

        'Regular' as tipo_registro

    from {{ ref('int_cdi__controle_interno_externo') }}

),

derivados as (

    select
        d.processorio as processorio_sei,
        d.subsecretaria_setor as subsecretaria,
        d.data_de_emissao as data_de_entrada,
        d.vencimento as data_vencimento,
        d.data_da_ultima_atualizacao,
        d.status,
        p.orgao_demandante,
        coalesce(p.manifestacao, 'Sem informação') as manifestacao,
        coalesce(p.tipo_de_demanda, 'Sem informação') as tipo_de_demanda,
        p.unidade_ap,
        d.observacao,
        d.derivado_do_processorio,
        d.breve_descricao_da_solicitacao,

        -- Status do prazo
        case
            when d.vencimento is null then 'Sem Prazo Definido'
            when d.status in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem') -- Regra de negócio
             and d.data_da_ultima_atualizacao is not null
                then case
                    when d.data_da_ultima_atualizacao <= d.vencimento then 'Dentro do Prazo'
                    else 'Fora do Prazo'
                end
            when current_date() <= d.vencimento then 'Dentro do Prazo'
            else 'Fora do Prazo'
        end as status_prazo,

        -- Tempo de atendimento
        case
            when d.status in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
             and d.data_da_ultima_atualizacao is not null
             and d.data_de_emissao is not null
                then date_diff(d.data_da_ultima_atualizacao, d.data_de_emissao, day)
        end as tempo_atendimento,

        -- Dias para vencer
        case
            when d.status not in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
             and d.vencimento is not null
                then date_diff(d.vencimento, current_date(), day)
        end as dias_para_vencer,

        'Derivado' as tipo_registro -- FLag de derivados - para separar os registros

    from {{ ref('int_cdi__controle_interno_externo_derivados') }} d
    left join (
        select distinct
            processorio_sei,
            orgao_demandante,
            manifestacao,
            tipo_de_demanda,
            unidade_ap
        from {{ ref('int_cdi__controle_interno_externo') }}
    ) p
        on d.processorio = p.processorio_sei

),

final as (

    select
        *,
        -- Situação descritiva
        case
            when status in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
                then 'Concluído ou Arquivado'
            when dias_para_vencer is null
                then 'Sem prazo definido'
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

        -- Flag de vencimento iminente -- tentando auxiliar em analises possíveis
        case
            when status not in ('Arquivado', 'Concluído - Devolvido ao Setor de Origem')
             and dias_para_vencer is not null
             and dias_para_vencer between 0 and 7
                then true
            else false
        end as vencimento_iminente

    from (
        select * from regulares
        union all
        select * from derivados
    )

)

select * from final