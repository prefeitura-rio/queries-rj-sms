{{
    config(
        alias='estabelecimento',
        materialized='table',
    )
}}

with 

estabelecimentos as (
    select 
    id_cnes as cnes,
    {{proper_estabelecimento('nome_limpo')}} as nome,
    {{proper_br('endereco_bairro')}} as bairro,
    
    case 
        -- Correção manual para o CAPSi Carim
        when id_cnes = '2698846' then '21'
        else area_programatica
    end as area_programatica,
    
    case 
        -- Correção manual para o CAPSi Carim
        when id_cnes = '2698846' then 'CENTRO DE ATENCAO PSICOSSOCIAL'
        else tipo_sms
    end as tipo,

    -- Correção manual para CF Arthur Bispo do Rosário e CF Wilma Costa com coord erradas
    case
        when id_cnes = '9071385' then cast('-22.937384589726616' as float64)
        when id_cnes = '9072659' then cast('-22.795039189601905' as float64)
        else endereco_latitude
    end as latitude,
    
    case 
        when id_cnes = '9071385' then cast('-43.39001040184862' as float64)
        when id_cnes = '9072659' then cast('-43.17581616941759' as float64)
        else endereco_longitude
    end as longitude,

    from {{ref('dim_estabelecimento')}}
) 

select * 
from estabelecimentos

LIMIT 1000