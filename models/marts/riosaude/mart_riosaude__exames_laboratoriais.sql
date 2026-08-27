{{
    config(
        alias="exames_laboratoriais",
        materialized="table",
        partition_by={
            "field": "data_particao",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by='estabelecimento_nome'
    )
}}

with
    source as (
        select *
        from {{ ref('raw_rmd__exames_laboratoriais') }}
        where upper(trim(estabelecimento_nome)) in (
            'HOSPITAL FEDERAL DE ANDARAI',
            'CER BARRA',
            'UPA JOAO XXIII',
            'UPA COSTA BARROS',
            'UPA CIDADE DE DEUS',
            'UPA DEL CASTILHO',
            'UPA MADUREIRA',
            'UPA VILA KENNEDY',
            'HOSPITAL RONALDO GAZOLLA',
            'UPA MAGALHÃES BASTOS',
            'UPA SEPETIBA',
            'UPA SENADOR CAMARÁ',
            'UPA ROCHA MIRANDA',
            'MATERNIDADE ROCINHA'
        )
    ),

    final as (
        select
          *
        from source
    )

select *
from final
