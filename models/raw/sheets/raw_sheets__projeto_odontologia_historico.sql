{{    
    config(
        schema="brutos_sheets",
        alias="projeto_odontologia_historico",
        tags=["daily"],
    )
}}


    select * from {{ source("brutos_sheets_staging", "projeto_odontologia_historico") }}