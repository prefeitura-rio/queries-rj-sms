{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'controle_interno_externo_filtros'
    ) 
}}

with regulares as (

    select
        subsecretaria___setor,
        data_de_entrada,
        coalesce(manifestacao, 'Sem informação') as manifestacao,
        status,
        unidade_ap,
        vencimento_1,
        coalesce(relator_auditor, 'Sem informação ou Sem Relator') as relator_auditor,
        data_da_ultima_atualizacao,
        orgao_demandante,
        coalesce(tipo_de_demanda, 'Sem informação') as tipo_de_demanda,
        cast(id as string) as id,
        'Regular' as tipo_registro
    from {{ ref('int_cdi__controle_interno_externo') }}

),

pais as (

    -- enriquece os derivados com os mesmos atributos de filtro
    select distinct
        processorio_sei,
        orgao_demandante,
        manifestacao,
        tipo_de_demanda,
        unidade_ap
    from {{ ref('int_cdi__controle_interno_externo') }}

),

derivados as (
    -- criando a derivados com info da regular para conseguir filtrar
    select
        d.subsecretaria___setor,
        d.data_de_emissao as data_de_entrada,               
        coalesce(p.manifestacao, 'Sem informação') as manifestacao,
        d.status,
        p.unidade_ap,
        d.vencimento as vencimento_1,                      
        'Sem informação ou Sem Relator' as relator_auditor,
        d.data_da_ultima_atualizacao,
        p.orgao_demandante,
        coalesce(p.tipo_de_demanda, 'Sem informação') as tipo_de_demanda,
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
        subsecretaria___setor,
        data_de_entrada,
        manifestacao,
        status,
        unidade_ap,
        vencimento_1,
        relator_auditor,
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