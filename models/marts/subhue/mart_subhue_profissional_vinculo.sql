{{ config(
    schema = "projeto_subhue",
    alias = "profissional_vinculo",
    materialized = "table",
    partition_by={
            "field": "cpf_particao",
            "data_type": "int64",
            "range": {"start": 0, "end": 100000000000, "interval": 34722222},
        }
) }}


with 
    estabelecimento_sms as (
        select 
            id_unidade,
            id_cnes,
            nome_acentuado,
            tipo,
            area_programatica
        from {{ref("dim_estabelecimento")}}
    ),

    profissional as (
        select 
            id_profissional_cnes,
            cpf,
            cns,
            nome
        from {{ref("int_gdb_cnes__profissional")}}
    ),

    vinculo_detalhe as (
        select *
        from {{ref("int_gdb_cnes__vinculo_detalhe")}}
    ),

    ocupacao as (
        select 
            id_cbo,
            left(id_cbo, 4) as id_cbo_familia,
            cbo.descricao as cbo_descricao,
            cbofam.descricao as familia_descricao
        from {{ref("raw_datasus__cbo")}} cbo
        left join {{ref("raw_datasus__cbo_fam")}} cbofam on left(cbo.id_cbo, 4) = cbofam.id_cbo_familia
    ),

    final as (
        select 
            id_profissional_cnes,

            -- Profissional
            struct(
                cpf,
                cns,
                nome,
                conselho_numero_registro as conselho_registro,
                uf_crm as conselho_uf
            ) as profissional,
            
            -- Estabelecimento
            struct(
                id_cnes,
                nome_acentuado as nome,
                tipo,
                area_programatica
            ) as estabelecimento,
            
            -- Ocupação 
            struct(
                id_cbo_familia,
                familia_descricao as familia,
                id_cbo,
                cbo_descricao as ocupacao
            ) as ocupacao,
            
            -- Vínculo
            struct(
                carga_horaria_hospitalar,
                carga_horaria_ambulatorial,
                carga_horaria_outros,
                descricao_vinculo as vinculo,
                descricao_subvinculo as subvinculo,
                descricao_vinculacao as vinculacao,
                descricao_conceito as conceito,
                vd.habilitado as ativo
            ) as vinculo,

            data_ultima_atualizacao as atualizado_em,
            data_particao as carregado_em,
            current_datetime() as processado_em,
            safe_cast(profissional.cpf as int64) as cpf_particao
        from {{ref("int_gdb_cnes__vinculo")}} v
        left join profissional using(id_profissional_cnes)
        left join estabelecimento_sms using(id_unidade)
        left join vinculo_detalhe vd using(id_vinculo)
        left join ocupacao using(id_cbo)
    )

select
    {{
        dbt_utils.generate_surrogate_key(
            [
                "cpf",
                "id_cnes",
                "id_cbo",
            ]
        )
    }} as id,
    *
from final
where 
    estabelecimento.nome is not null 
    and vinculo.ativo is true

