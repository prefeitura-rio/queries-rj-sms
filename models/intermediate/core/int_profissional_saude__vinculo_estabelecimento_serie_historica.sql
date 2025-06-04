-- cria serie historica de vinculacao nos estabelecimentos de saude da atenção
-- primaria do rio de janeiro
with
    profissional_sus as (
        select
            id_codigo_sus,
            nome,
            cns,
            data_atualizacao,
            row_number() over (
                partition by nome, id_codigo_sus, cns order by data_atualizacao desc
            ) as ordenacao
        from {{ ref("raw_cnes_web__dados_profissional_sus") }}
        where cns != ""
    ),
    
    profissional_web as (
         select 
            id_unidade,
            id_profissional_sus,
            id_cbo,
            atende_sus_indicador,
            substring(vinculacao, 1, 4) as id_tipo_vinculo, 
            substring(vinculacao, 1, 2) as id_vinculacao,
            left(id_cbo, 4) as id_cbo_familia,
            carga_horaria_ambulatorial,
            carga_horaria_hospitalar,
            carga_horaria_outros,
            conselho_tipo as id_tipo_conselho,
            id_registro_conselho,
            sigla_uf_crm,
            preceptor_indicador,
            residente_indicador,
            cnpj_empregador,
            data_atualizacao,
            row_number() over (
            partition by 
                id_unidade, 
                id_profissional_sus
            order by data_atualizacao desc
            ) as ordenacao
         from {{ ref("raw_cnes_web__carga_horaria_sus") }}
         where atende_sus_indicador is true
    ),

     estabelecimento as (
        select * 
        from {{ ref("raw_cnes_web__estabelecimento") }}
        where id_estado_gestor = '33' -- RJ
     ),
    
    cbo as (
        select * 
        from {{ ref("raw_datasus__cbo") }}
    ),

    cbo_fam as (
        select * 
        from {{ ref("raw_datasus__cbo_fam") }}
    ),

    tipo_vinculo as (
        select
            concat(id_vinculacao, tipo) as codigo_tipo_vinculo,
            descricao,
            row_number() over (
                partition by concat(id_vinculacao, tipo), descricao
                order by data_carga desc
            ) as ordenacao
        from {{ ref("raw_cnes_web__tipo_vinculo") }}
    ),

    vinculo as (
        select
            id_vinculacao,
            descricao,
            row_number() over (
                partition by id_vinculacao, descricao order by data_carga desc
            ) as ordenacao
        from {{ ref("raw_cnes_web__vinculo") }}
    )

select

    e.id_cnes as id_cnes,
    cod_sus.id_codigo_sus as profissional_codigo_sus,
    cod_sus.cns as profissional_cns,
    cod_sus.nome as profissional_nome,
    vinculacao.descricao as vinculacao,
    tipo_vinculo.descricao as vinculo_tipo,
    p.id_cbo,
    ocup.descricao as cbo,
    p.id_cbo_familia,
    ocupf.descricao as cbo_familia,
    p.id_registro_conselho,
    p.id_tipo_conselho,
    p.carga_horaria_outros,
    p.carga_horaria_hospitalar,
    p.carga_horaria_ambulatorial,
    cod_sus.data_atualizacao

from profissional_web as p
left join estabelecimento as e on p.id_unidade = e.id_unidade
left join cbo as ocup on p.id_cbo = ocup.id_cbo
left join cbo_fam as ocupf on left(p.id_cbo_familia, 4) = ocupf.id_cbo_familia
left join
    (select * from tipo_vinculo where ordenacao = 1) as tipo_vinculo
    on p.id_tipo_vinculo = tipo_vinculo.codigo_tipo_vinculo
left join
    (select * from vinculo where ordenacao = 1) as vinculacao
    on p.id_vinculacao = vinculacao.id_vinculacao
left join
    (select * from profissional_sus where ordenacao = 1) as cod_sus
    on p.id_profissional_sus = cod_sus.id_codigo_sus
qualify row_number() over (
    partition by id_cnes, id_profissional_sus 
    order by data_atualizacao desc) = 1
