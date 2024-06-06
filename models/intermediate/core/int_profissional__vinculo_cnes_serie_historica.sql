select
    concat(p.ano, '-', lpad(cast(p.mes as string), 2, '0')) data_registro,
    p.id_estabelecimento_cnes,
    utils.clean_name_string(p.nome) as nome_profissional,
    utils.clean_numeric_string(p.id_registro_conselho) as id_registro_conselho,
    p.tipo_conselho,
    cod_sus.codigo_sus as cod_profissional_sus,
    utils.clean_numeric_string(p.cartao_nacional_saude) as cartao_nacional_saude,
    ocup.ds_cbo as cbo_descricao,
    ocupf.ds_regra as cbo_fam_descricao,
    vinculacao.descricao_vinculacao as vinculacao_descricao,
    tipo_vinculo.descricao_vinculacao as tipo_vinculo_descricao,
    p.carga_horaria_outros,
    p.carga_horaria_hospitalar,
    p.carga_horaria_ambulatorial

from {{ ref("raw_cnes_ftp__profissional") }} as p
left join {{ ref("raw_datasus__cbo") }} as ocup on p.cbo_2002 = ocup.cbo
left join
    {{ ref("raw_datasus__cbo_fam") }} as ocupf on left(p.cbo_2002, 4) = ocupf.chave

left join
    (
        select *
        from
            (
                select
                    concat(codigo_vinculacao, tipo_vinculacao) as cd_tipo_vinculo,
                    descricao_vinculacao,
                    row_number() over (
                        partition by
                            concat(codigo_vinculacao, tipo_vinculacao),
                            descricao_vinculacao
                        order by data_carga desc
                    ) as ordenacao
                from {{ ref("raw_cnes_web__tipo_vinculo") }}
            )
        where ordenacao = 1
    ) as tipo_vinculo
    on substring(p.tipo_vinculo, 1, 4) = tipo_vinculo.cd_tipo_vinculo

left join
    (
        select *
        from
            (
                select
                    codigo_vinculacao,
                    descricao_vinculacao,
                    row_number() over (
                        partition by codigo_vinculacao, descricao_vinculacao
                        order by data_carga desc
                    ) as ordenacao
                from {{ ref("raw_cnes_web__vinculo") }}
            )
        where ordenacao = 1
    ) as vinculacao
    on substring(p.tipo_vinculo, 1, 2) = vinculacao.codigo_vinculacao

left join
    (
        select *
        from
            (
                select
                    nome,
                    codigo_sus,
                    cns,
                    data_atualizacao,
                    row_number() over (
                        partition by nome, codigo_sus, cns
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
