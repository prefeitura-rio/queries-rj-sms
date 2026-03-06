with 
    estabelecimento as (
        select 
            id_unidade,
            id_cnes,
        from rj-sms-dev.Herian__intermediario_gdb_cnes.int_gdb_cnes__estabelecimento
    ),

    profissional as (
        select 
            id_profissional_cnes,
            cpf
        from rj-sms-dev.Herian__intermediario_gdb_cnes.int_gdb_cnes__profissional
    ),

    vinculo_detalhe as (
        select *
        from rj-sms-dev.Herian__intermediario_gdb_cnes.int_gdb_cnes__vinculo_detalhe
    )

select 
    id_profissional_cnes,
    cpf,
    id_cbo,
    carga_horaria_hospitalar,
    carga_horaria_ambulatorial,
    carga_horaria_outros,
    conselho_numero_registro as conselho_numero,
    uf_crm as conselho_uf,
    vinculo,
    vinculacao,
    habilitado,
    descricao_subvinculo as subvinculo,
    descricao_conceito as conceito,
    status_vinculo,
    habilitado,
    id_cnes,
    v.*,
    vinculo_detalhe.*,
    data_particao
from rj-sms-dev.Herian__intermediario_gdb_cnes.int_gdb_cnes__vinculo v
left join profissional using(id_profissional_cnes)
left join estabelecimento using(id_unidade)
left join vinculo_detalhe using(id_vinculo)