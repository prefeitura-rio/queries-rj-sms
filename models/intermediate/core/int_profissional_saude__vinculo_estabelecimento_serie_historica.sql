-- cria serie historica de vinculacao nos estabelecimentos de saude da atenção
-- primaria do rio de janeiro
select
    concat(p.ano, '-', lpad(cast(p.mes as string), 2, '0')) data_registro,
    p.id_estabelecimento_cnes as id_cnes,
    cod_sus.id_codigo_sus as profissional_codigo_sus,
    utils.clean_numeric_string(p.cartao_nacional_saude) as profissional_cns,
    utils.clean_name_string(p.nome) as profissional_nome,
    vinculacao.descricao as vinculacao,
    tipo_vinculo.descricao as vinculo_tipo,
    ocup.descricao as cbo,
    ocupf.descricao as cbo_familia,
    utils.clean_numeric_string(p.id_registro_conselho) as id_registro_conselho,
    p.tipo_conselho as id_tipo_conselho,
    p.carga_horaria_outros,
    p.carga_horaria_hospitalar,
    p.carga_horaria_ambulatorial

from {{ ref("raw_cnes_ftp__profissional") }} as p
left join {{ ref("raw_datasus__cbo") }} as ocup on p.cbo_2002 = ocup.id_cbo
left join
    {{ ref("raw_datasus__cbo_fam") }} as ocupf
    on left(p.cbo_2002, 4) = ocupf.id_cbo_familia

left join
    (
        select *
        from
            (
                select
                    concat(id_vinculacao, tipo) as codigo_tipo_vinculo,
                    descricao,
                    row_number() over (
                        partition by concat(id_vinculacao, tipo), descricao
                        order by data_carga desc
                    ) as ordenacao
                from {{ ref("raw_cnes_web__tipo_vinculo") }}
            )
        where ordenacao = 1
    ) as tipo_vinculo
    on substring(p.tipo_vinculo, 1, 4) = tipo_vinculo.codigo_tipo_vinculo

left join
    (
        select *
        from
            (
                select
                    id_vinculacao,
                    descricao,
                    row_number() over (
                        partition by id_vinculacao, descricao order by data_carga desc
                    ) as ordenacao
                from {{ ref("raw_cnes_web__vinculo") }}
            )
        where ordenacao = 1
    ) as vinculacao
    on substring(p.tipo_vinculo, 1, 2) = vinculacao.id_vinculacao

left join
    (
        select *
        from
            (
                select
                    id_codigo_sus,
                    nome,
                    cns,
                    data_atualizacao,
                    row_number() over (
                        partition by nome, id_codigo_sus, cns
                        order by data_atualizacao desc
                    ) as ordenacao
                from {{ ref("raw_cnes_web__dados_profissional_sus") }}
            )
        where ordenacao = 1
    ) as cod_sus
    on cartao_nacional_saude = cod_sus.cns

where
    ano >= 2008
    and sigla_uf = "RJ"
    and (
        indicador_atende_sus = 1
        or indicador_vinculo_contratado_sus = 1
        or indicador_vinculo_autonomo_sus = 1
    )
    and id_estabelecimento_cnes
    in (select distinct id_cnes from {{ ref("dim_estabelecimento") }})
