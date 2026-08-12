{{
    config(
        alias='ultimos_episodios',
        materialized='table',
        description='A data da última atualização do episódio assistencial de cada prontuário'
    )
}}

SELECT
  case prontuario.fornecedor
    when 'pcsm' then 'PCSM'
    when 'sarah' then 'SARAH'
    when 'mv' then 'MV'
    when 'prontuaRio' then 'ProntuaRio'
    when 'vitai' then 'TiMed'
    when 'vitacare' then 'Vitacare'
    else upper(prontuario.fornecedor)
  end as fornecedor,
  cast(MAX(entrada_data) as datetime) as ultima_atualizacao,
  DATE_DIFF(CURRENT_DATETIME('America/Sao_Paulo'), DATETIME(MAX(entrada_data)), DAY) as dias_sem_atualizar
FROM {{ref('mart_historico_clinico__episodio')}}d
GROUP BY 1