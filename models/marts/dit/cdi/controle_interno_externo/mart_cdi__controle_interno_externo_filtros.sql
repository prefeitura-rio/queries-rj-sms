{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'controle_interno_externo_filtros'
    ) 
}}

with regulares as (

    select
        coalesce(subsecretaria_setor, 'Sem informação') as subsecretaria_setor,
        data_de_entrada,
        coalesce(manifestacao, 'Sem informação') as manifestacao,
        status,
        unidade_ap,
        vencimento_1,
        coalesce(relator_auditor, 'Sem informação ou Sem Relator') as relator_auditor,
        coalesce(data_da_ultima_atualizacao, null) as data_da_ultima_atualizacao, -- pra evitar nulls na análise de prazo
        coalesce(orgao_demandante, 'Sem informação') as orgao_demandante,
        coalesce(tipo_de_demanda, 'Sem informação') as tipo_de_demanda,
        id,
        'Regular' as tipo_registro
    from {{ ref('int_cdi__controle_interno_externo') }}

),

pais as (

    -- enriquece os derivados com os mesmos atributos de filtro
    select
        processorio_sei,
        coalesce(orgao_demandante, 'Sem informação') as orgao_demandante,
        coalesce(manifestacao, 'Sem informação') as manifestacao,
        coalesce(tipo_de_demanda, 'Sem informação') as tipo_de_demanda,
        coalesce(unidade_ap, 'Sem informação') as unidade_ap,
        coalesce(relator_auditor, 'Sem informação ou Sem Relator') as relator_auditor
    from {{ ref('int_cdi__controle_interno_externo') }}

),

derivados as (
    -- criando a derivados com info da regular para conseguir filtrar
    select
        d.subsecretaria_setor,
        d.data_de_emissao as data_de_entrada,         
        p.manifestacao,
        d.status,
        p.unidade_ap,
        d.vencimento as vencimento_1,
        p.relator_auditor,
        d.data_da_ultima_atualizacao,
        p.orgao_demandante,
        p.tipo_de_demanda,
        cast(d.id as string) as id,
        'Derivado' as tipo_registro
    from {{ ref('int_cdi__controle_interno_externo_derivados') }} d
    left join pais p
        on d.processorio = p.processorio_sei

),

base_union as (

    select * from regulares
    union all
    select * from derivados

),

base_tratada as (

    select
        subsecretaria_setor,
        data_de_entrada,
        case when manifestacao is null then 'Sem informação' else manifestacao end as manifestacao,
        status,
        case when unidade_ap is null then 'Sem informação' else unidade_ap end as unidade_ap,
        vencimento_1,
        case when relator_auditor is null then 'Sem informação ou Sem Relator' else relator_auditor end as relator_auditor,
        data_da_ultima_atualizacao,
        orgao_demandante,
        tipo_de_demanda,

        -- total geral (regulares + derivados) isso é feito pra evitar que os ids sejam iguais 
        count(concat(tipo_registro, '|', id)) as total_processos,
        count(if(tipo_registro = 'Regular', id, null)) as total_regulares,
        count(if(tipo_registro = 'Derivado', id, null)) as total_derivados

    from base_union
    group by 1,2,3,4,5,6,7,8,9,10

)

select * from base_tratada