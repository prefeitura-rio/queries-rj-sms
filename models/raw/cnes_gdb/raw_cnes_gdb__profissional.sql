{{
    config(
        alias="profissional",
        schema= "brutos_cnes_gdb"
    )
}}

with source as (
      select * from {{ source('brutos_cnes_gdb_staging', 'profissional') }}
),
renamed as (
select 
    cast(id_profissional_sus as string) as id_profissional_sus,
    cast(PROF_ID as string) as id_profissional_cnes,
    cast(CPF_PROF as string) as cpf,
    cast(NOME_PROF as string) as nome,
    cast(DATA_NASC as date) as data_nascimento,	
    case 
        when SEXO='F' then 'female'
        when SEXO='M' then 'male'
        else null
    end as sexo,
    cast(COD_CNS as string) as cns
    from source
)
select * from renamed	