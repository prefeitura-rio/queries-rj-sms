{{    
    config(
        schema="brutos_sheets",
        alias="projeto_odontologia_sigtap_odo",
        tags=["daily"],
    )
}}


    select * from {{ source("brutos_sheets_staging", "projeto_odontologia_sigtap_odo") }}