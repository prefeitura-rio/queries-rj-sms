{{
    config(
        schema="saude_dados_mestres",
        alias="equipe",
        materialized="table",
    )
}}


SELECT  
  equipe.CO_EQUIPE as cod_equipe,
  equipe.CO_AREA as cod_area,
  equipe.SEQ_EQUIPE as seq_equipe,
  equipe.NO_REFERENCIA as nome_equipe,
  equipe.TP_EQUIPE as tipo_equipe,
  equipe.CO_SUB_TIPO_EQUIPE as subtipo_equipe,
  equipe.CO_UNIDADE as cod_unidade_saude, 
  equipe.CO_PROF_SUS_PRECEPTOR as cod_profissional_preceptor,
  lista_profissionais.profissionais,
  equipe.DT_ATUALIZACAO as ultima_atualizacao_infos_equipe,
  lista_profissionais.DT_ATUALIZACAO as ultima_atualizacao_profissionais_equipe,
FROM `rj-sms-dev.brutos_cnes_web_staging.tbEquipe` as equipe

LEFT JOIN (
    SELECT * 
    FROM (  
        SELECT *, 
        ROW_NUMBER() OVER (PARTITION BY CO_UNIDADE,SEQ_EQUIPE ORDER BY DT_ATUALIZACAO DESC) as ordenacao
        FROM  (
            SELECT 
            CO_UNIDADE, 
            SEQ_EQUIPE,
            DT_ATUALIZACAO, 
            ARRAY_AGG(CO_PROFISSIONAL_SUS) as profissionais
            FROM `rj-sms-dev.brutos_cnes_web_staging.rlEstabEquipeProf`
            WHERE _data_carga = (
                SELECT MAX(_data_carga)
                FROM `rj-sms-dev.brutos_cnes_web_staging.rlEstabEquipeProf`
            )
            AND CO_MUNICIPIO = '330455'
            GROUP BY 1,2,3
        )
    )
    WHERE ordenacao = 1
) as lista_profissionais
ON (
    lista_profissionais.CO_UNIDADE = equipe.CO_UNIDADE
    AND lista_profissionais.SEQ_EQUIPE = equipe.SEQ_EQUIPE
    )


WHERE equipe._data_carga = (
  SELECT MAX(_data_carga)
  FROM `rj-sms-dev.brutos_cnes_web_staging.tbEquipe`
)
AND equipe.CO_MUNICIPIO = '330455'