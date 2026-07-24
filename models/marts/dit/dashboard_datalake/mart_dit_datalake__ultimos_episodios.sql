{{
    config(
        alias='ultimos_episodios',
        materialized='table',
        description='A data da última atualização do episódio assistencial de cada prontuário'
    )
}}

SELECT
  case prontuario.fornecedor
    when 'pcsm' then 'Prontuário Carioca da Saúde Mental'
    when 'sarah' then 'SARAH'
    when 'mv' then 'MV'
    when 'prontuaRio' then 'ProntuaRio'
    when 'vitai' then 'TiMed'
    when 'vitacare' then 'Vitacare'
    else prontuario.fornecedor
  end as fornecedor,
  MAX(entrada_data) as ultima_atualizacao,
  DATE_DIFF(CURRENT_DATE('America/Sao_Paulo'), DATE(MAX(entrada_data)), DAY) as dias_sem_atualizar
FROM {{ref('mart_historico_clinico__episodio')}}
GROUP BY 1