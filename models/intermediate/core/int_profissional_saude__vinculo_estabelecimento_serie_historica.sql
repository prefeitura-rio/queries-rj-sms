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
    profissional_ftp as (
        select
            concat(ano, '-', lpad(cast(mes as string), 2, '0')) data_registro,
            id_estabelecimento_cnes,
            sigla_uf,
            {{clean_numeric_string('cartao_nacional_saude')}} as profissional_cns,
            {{clean_numeric_string('nome')}} as profissional_nome,
            cbo_2002 as id_cbo,
            substring(tipo_vinculo, 1, 4) as id_tipo_vinculo,
            substring(tipo_vinculo, 1, 2) as id_vinculacao,
            left(cbo_2002, 4) as id_cbo_familia,
            {{clean_numeric_string('id_registro_conselho')}} as id_registro_conselho,
            tipo_conselho as id_tipo_conselho,
            carga_horaria_outros,
            carga_horaria_hospitalar,
            carga_horaria_ambulatorial
        from {{ ref("raw_cnes_ftp__profissional") }}
        where
            ano >= 2008
            and sigla_uf = "RJ"
            and (
                indicador_atende_sus = 1
                or indicador_vinculo_contratado_sus = 1
                or indicador_vinculo_autonomo_sus = 1
            )
    ),
    cbo as (select * from {{ ref("raw_datasus__cbo") }}),
    cbo_fam as (select * from {{ ref("raw_datasus__cbo_fam") }}),
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
    p.data_registro,
    p.id_estabelecimento_cnes as id_cnes,
    cod_sus.id_codigo_sus as profissional_codigo_sus,
    p.profissional_cns,
    p.profissional_nome,
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
    p.carga_horaria_ambulatorial

from profissional_ftp as p
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
    on p.profissional_cns = cod_sus.cns
