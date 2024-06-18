{{ config(alias="paciente", tags=["hci"]) }}


with
    source as (
        select *
        from {{ source("brutos_historico_clinico_integrado_staging", "paciente") }}
    ),
    renamed as (
        select
            safe_cast(patient_code as string) as id_paciente,
            safe_cast(patient_cpf as string) as cpf,
            safe_cast(name as string) as nome,
            safe_cast(gender_id as int) as id_genero,
            safe_cast(race_id as int) as id_raca,
            safe_cast(birth_date as date format "YYYY-MM-DD") as data_nascimento,
            safe_cast(deceased as bool) as falecido,
            safe_cast(deceased_date as date format "YYYY-MM-DD") as data_obito,
            safe_cast(father_name as string) as nome_pai,
            safe_cast(mother_name as string) as nome_mae,
            safe_cast(birth_city_id as int) as id_cidade_nascimento,
            safe_cast(nationality_id as int) as id_nacionalidade,
            timestamp(created_at) as created_at,
            timestamp(updated_at) as updated_at,
        from source
    )
select *
from renamed
