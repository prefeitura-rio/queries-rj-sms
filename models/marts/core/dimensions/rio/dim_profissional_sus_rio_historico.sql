{{
    config(
        enabled=true,
        schema="saude_cnes",
        alias="profissional_sus_rio_historico",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "day",
        },
    )
}}

with
    versao_atual as (
        select max(data_particao) as versao from {{ ref("raw_cnes_web__tipo_unidade") }}
    ),

    dim_estabelecimentos_sus_rio_historico as (
        select *
        from {{ ref("dim_estabelecimento_sus_rio_historico") }}
        where safe_cast(data_particao as string) = (select versao from versao_atual)
    ),

    profissionais_mrj as (
        select * from {{ ref("int_profissional_sus_rio_historico__brutos_filtrados") }}
    ),

    cbo as (select * from {{ ref("raw_datasus__cbo") }}),

    cbo_fam as (select * from {{ ref("raw_datasus__cbo_fam") }}),

    tipo_vinculo as (
        select
            concat(id_vinculacao, tipo) as codigo_tipo_vinculo, descricao, data_particao
        from {{ ref("raw_cnes_web__tipo_vinculo") }}
        where data_particao = (select versao from versao_atual)
    ),

    vinculo as (
        select id_vinculacao, descricao
        from {{ ref("raw_cnes_web__vinculo") }}
        where data_particao = (select versao from versao_atual)
    ),

    id_sus as (
        select cns, id_codigo_sus
        from
            (
                select
                    cns,
                    id_codigo_sus,
                    data_atualizacao,
                    row_number() over (
                        partition by cns order by data_atualizacao desc
                    ) as rn
                from {{ ref("raw_cnes_web__dados_profissional_sus") }}
            )
        where rn = 1
    ),

    duracao_contrato as (
        select
            id_profissional_sus,
            data_entrada_profissional,
            data_desligamento_profissional
        from
            (
                select
                    id_profissional_sus,
                    data_entrada_profissional,
                    data_desligamento_profissional,
                    data_atualizacao,
                    row_number() over (
                        partition by id_profissional_sus order by data_atualizacao desc
                    ) as rn
                from {{ ref("raw_cnes_web__equipe_profissionais") }}
            )
        where rn = 1
    ),

    profissional_dados_hci as (
        select distinct cns, cpf from {{ ref("mart_historico_clinico__paciente") }}
    ),

    final as (
        select
            p.id_cnes,

            estabs.nome_fantasia as estabelecimento_nome_fantasia,
            estabs.esfera as estabelecimento_esfera,
            estabs.tipo_gestao_descr as estabelecimento_gestao,
            estabs.id_ap as id_estabelecimento_ap,
            estabs.ap as estabelecimento_ap,
            estabs.estabelecimento_sms_indicador,

            p.data_registro,
            hci.cpf as cpf_temp,
            id_sus.id_codigo_sus as profissional_codigo_sus,
            p.profissional_cns as cns,
            p.profissional_nome as nome,
            vinculacao.descricao as vinculacao,
            tipo_vinculo.descricao as vinculo_tipo,
            p.id_cbo,
            ocup.descricao as cbo,
            p.id_cbo_familia,
            ocupf.descricao as cbo_familia,
            id_registro_conselho,
            id_tipo_conselho,
            carga_horaria_outros,
            carga_horaria_hospitalar,
            carga_horaria_ambulatorial,
            carga_horaria_total,

            p.ano_competencia,
            p.mes_competencia,
            parse_date('%Y-%m-%d', tipo_vinculo.data_particao) as data_particao

        from profissionais_mrj as p
        left join
            dim_estabelecimentos_sus_rio_historico as estabs using (
                ano_competencia, mes_competencia, id_cnes
            )
        left join
            profissional_dados_hci as hci
            on safe_cast(p.profissional_cns as int64)
            = (select safe_cast(cns as int64) from unnest(hci.cns) as cns limit 1)
        left join cbo as ocup on p.id_cbo = ocup.id_cbo
        left join cbo_fam as ocupf on left(p.id_cbo_familia, 4) = ocupf.id_cbo_familia
        left join tipo_vinculo on p.id_tipo_vinculo = tipo_vinculo.codigo_tipo_vinculo
        left join vinculo as vinculacao on p.id_vinculacao = vinculacao.id_vinculacao
        left join id_sus on p.profissional_cns = id_sus.cns
    ),

    enriquecimento_cpf as (
        select final.*, coalesce(final.cpf_temp, aux_cpfs.cpf[ordinal(1)]) as cpf_enriq

        from final

        left join
            {{ ref("raw_sheets__profissionais_cns_cpf_aux") }} as aux_cpfs
            on safe_cast(final.cns as int64) = safe_cast(aux_cpfs.cns as int64)
    ),

    enriquecimento_cpf_gdb as (
        select ec.* except (cpf_temp), coalesce(ec.cpf_enriq, gdb.cpf) as cpf
        from enriquecimento_cpf as ec

        left join
            (
                select distinct safe_cast(cns as int64) as cns, cpf
                from {{ ref("raw_cnes_gdb__profissional") }}
            ) as gdb

            on safe_cast(ec.cns as int64) = gdb.cns
    ),

    enriquecimento_duracao_contrato as (
        select
            ec.* except (cpf_enriq),
            dc.data_entrada_profissional,
            dc.data_desligamento_profissional
        from enriquecimento_cpf_gdb as ec
        left join
            duracao_contrato as dc
            on ec.profissional_codigo_sus = dc.id_profissional_sus

    )

select *
from enriquecimento_duracao_contrato
