{{ 
    config(
        materialized = 'table',
        schema = 'projeto_cdi',
        alias = 'equipe_tutela_individual_filtros'
    ) 
}}

with base as (

    SELECT 
        area, 
        assuntos,
        data_de_entrada,
        classificacao_idade as faixa_idade,
        case
            when (
                regexp_contains(upper(sexo), r'M')
                and regexp_contains(upper(sexo), r'F')
            )
            or upper(trim(sexo)) = 'AMBOS'
                then 'Nucleo Familiar'

            -- Núcleo familiar (NF explícito)
            when upper(trim(sexo)) = 'NF'
                then 'Nucleo Familiar'

            -- Feminino
            when upper(trim(sexo)) = 'F'
                then 'Feminino'

            -- Masculino
            when upper(trim(sexo)) = 'M'
                then 'Masculino'

            -- Qualquer outro valor
            else 'Não identificado'
        end as sexo_tratado,
 
        orgao,
        orgao_para_subsidiar, 

        case
            when upper(orgao) like 'MP%' then 'Ministério Público'
            when upper(orgao) like 'DP%' then 'Defensoria Pública'
            else 'Outros'
        end as orgao_mp_dp,
        situacao,
        count(processo_rio) total_procesos
    from {{ ref('int_cdi__equipe_tutela_individual') }}
    
    group by 1,2,3,4,5,6,7,8,9
    order by 10 desc



    

)

select * from base