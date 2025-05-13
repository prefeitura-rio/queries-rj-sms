{{
    config(
        alias="vinculo",
        schema= "brutos_cnes_gdb"
    )
}}

with source as (
      select * from {{ source('brutos_cnes_gdb_staging', 'vinculo') }}
),
renamed as (
    select
    cast(id_profissional_sus as string) as id_profissional_sus,	
    cast(UNIDADE_ID as string) as id_unidade,
    cast(PROF_ID as string) as id_profissional_cnes,
    cast(COD_CBO as string) as id_cbo,
    if(TP_SUS_NAO_SUS='S',true,false) as atende_sus,
    cast(IND_VINC as string) as id_vinculo,	
    cast(NU_CNPJ_DET_VINC as string) as empregador_cnpj,	
    cast(CGHORAOUTR as integer) as carga_horaria_outros,
    cast(CG_HORAAMB as integer) as carga_horaria_ambulatorial,
    cast(CGHORAHOSP as integer) as carga_horaria_hospitalar,	
    cast(CONSELHOID as string) as id_conselho,
    cast(N_REGISTRO as string) as conselho_numero_registro,	
    cast(SG_UF_CRM as string) as uf_crm,
    if(TP_PRECEPTOR='1',true,false) as eh_preceptor,		
    if(TP_RESIDENTE='1',true,false) as eh_residente,
    case 
        when STATUSMOV='1' then 'Não aprovado'
        when STATUSMOV='2' then 'Consistido'
        when STATUSMOV='3' then 'Exportado'
        else null
    end as status_vinculo,
    cast(DATA_ATU as date) as data_ultima_atualizacao,
    cast(USUARIO as string) as usuario_atualizador
    from source
)
select * from renamed
  