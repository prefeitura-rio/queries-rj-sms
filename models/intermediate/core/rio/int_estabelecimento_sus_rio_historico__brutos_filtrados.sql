WITH

-- Obtendo a data mais atual
versao_atual AS (
    SELECT MAX(data_particao) AS versao 
    FROM {{ ref("raw_cnes_web__tipo_unidade") }}
), -- OBS: Não faz mais sentido pegar a versão de alguma outra tabela, sem ser tipo_unidade?

-- Obtendo todos os estabelecimentos do MRJ que possuem vinculo com o SUS
estabelecimentos_brutos_non_unique AS (
    SELECT
        ano,
        mes,
        cep,
        id_estabelecimento_cnes,
        id_natureza_juridica,
        tipo_gestao,
        tipo_unidade,
        tipo_turno,
        indicador_vinculo_sus as vinculo_sus_indicador,
        indicador_atendimento_internacao_sus as atendimento_internacao_sus_indicador,	
        indicador_atendimento_ambulatorial_sus as atendimento_ambulatorial_sus_indicador,
        indicador_atendimento_sadt_sus as atendimento_sadt_sus_indicador,
        indicador_atendimento_urgencia_sus as atendimento_urgencia_sus_indicador,  
        indicador_atendimento_outros_sus as atendimento_outros_sus_indicador, 
        indicador_atendimento_vigilancia_sus as atendimento_vigilancia_sus_indicador,
        indicador_atendimento_regulacao_sus as atendimento_regulacao_sus_indicador
    FROM {{ ref("raw_cnes_ftp__estabelecimento") }}
    WHERE 
        ano >= 2010 
        AND sigla_uf = "RJ"
        AND id_municipio_6 = "330455"
        AND (
            indicador_vinculo_sus = 1
            OR indicador_atendimento_internacao_sus = 1 	
            OR indicador_atendimento_ambulatorial_sus = 1
            OR indicador_atendimento_sadt_sus = 1
            OR indicador_atendimento_urgencia_sus = 1 
            OR indicador_atendimento_outros_sus = 1
            OR indicador_atendimento_vigilancia_sus = 1
            OR indicador_atendimento_regulacao_sus = 1
        )
)

select distinct * from estabelecimentos_brutos_non_unique