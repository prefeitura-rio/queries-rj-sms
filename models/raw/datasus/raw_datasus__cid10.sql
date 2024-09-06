{{
    config(
        alias="cid10",
        schema = "brutos_datasus"
    )
}}
select * from 
{{ source("brutos_datasus_staging", "cid10") }}