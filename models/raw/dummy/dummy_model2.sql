{{
    config(
        alias="dummy_model2",
        materialized="table",
        tags=["dummy", "test", "subgeral"],
        meta={
            "owner": "herian",
            "team": "subgeral"
            }
    )
}}

select "Teste" as coluna_teste