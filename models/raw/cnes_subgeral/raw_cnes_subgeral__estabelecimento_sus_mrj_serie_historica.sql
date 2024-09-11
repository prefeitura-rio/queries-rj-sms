{{
    config(
        schema="raw_cnes_subgeral__estabelecimento_sus_mrj_serie_historica",
        alias="brutos_estabelecimento_sus_mrj_serie_historica"
    )
}}

with source as (select * from {{  source("brutos_cnes_ftp", "estabelecimento")  }} )

select
    ano,
    mes,
    cep,
    id_estabelecimento_cnes,
    id_natureza_juridica,
    tipo_gestao,
    tipo_unidade,
    tipo_turno,
    indicador_vinculo_sus,
    indicador_atendimento_internacao_sus,	
    indicador_atendimento_ambulatorial_sus,
    indicador_atendimento_sadt_sus,
    indicador_atendimento_urgencia_sus,  
    indicador_atendimento_outros_sus, 
    indicador_atendimento_vigilancia_sus,
    indicador_atendimento_regulacao_sus

from
    source
    
where 
    sigla_uf = "RJ"
    and id_municipio_6 = "330455"
    and (
        indicador_vinculo_sus = 1
        or indicador_atendimento_internacao_sus = 1 	
        or indicador_atendimento_ambulatorial_sus = 1
        or indicador_atendimento_sadt_sus = 1
        or indicador_atendimento_urgencia_sus = 1 
        or indicador_atendimento_outros_sus = 1
        or indicador_atendimento_vigilancia_sus = 1
        or indicador_atendimento_regulacao_sus = 1
    )   