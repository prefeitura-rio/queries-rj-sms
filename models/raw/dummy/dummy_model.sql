{{
    config(
        alias="dummy_model",
        materialized="table",
        tags=["dummy", "test", "cit"],
        meta={
            "owner": ["avellar", "herian", "daniel", "karen"],
            "dominio": "cit"
            }
    )
}}

select "Teste" as coluna_teste