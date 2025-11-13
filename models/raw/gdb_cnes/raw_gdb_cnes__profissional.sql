{{
    config(
        alias="profissional",
        schema= "brutos_gdb_cnes"
    )
}}

with source as (
    select * from {{ source('brutos_gdb_cnes_staging', 'LFCES018') }}
),
renamed as (
    select
        substr(upper(to_hex(md5(cast(PROF_ID as string)))), 0, 16) as id_profissional_sus,
        cast({{process_null('PROF_ID')}} as string) as id_profissional_cnes,
        cast({{process_null('CPF_PROF')}} as string) as cpf,
        cast({{process_null('COD_CNS')}} as string) as cns,
        cast({{process_null('NOME_PROF')}} as string) as nome,
        safe_cast({{process_null('DATA_NASC')}} as date) as data_nascimento,
        case 
            when SEXO='F' then 'Feminino'
            when SEXO='M' then 'Masculino'
            else null
        end as sexo
    from source
)
select *
from renamed
