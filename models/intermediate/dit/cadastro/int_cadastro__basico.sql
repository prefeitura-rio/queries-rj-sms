{{
    config(
        alias="cadastro_basico",
        materialized="table",
    )
}}

with
    source as (
        select 
            cpf.cpf,
            upper(nome) as nome,
            upper(mae_nome) as mae_nome,
            nascimento_data,
            sexo,
            case when obito_ano is not null then true else false end as obito_indicador,
        from {{ ref("raw_bcadastro__cpf") }}
        where atualizacao_data > '2020-01-01' 
            and nascimento_data is not null 
            and mae_nome is not null
    )

select *
from source