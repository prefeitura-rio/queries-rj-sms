with
    source as (
        select * from {{ source("brutos_plataforma_smsrio", "profissional_saude_cpf") }}
    ),
    renamed as (
        select
            safe_cast(cpf as string) as cpf,
            safe_cast(cns_primario as string) as cns_primario,
            safe_cast(cns_procurado as string) as cns_procurado,
            safe_cast(cns_provisorio as string) as cns_provisorio,
            safe_cast(nome_profissional as string) as profissional_nome,
        from source
    )
select *
from renamed
