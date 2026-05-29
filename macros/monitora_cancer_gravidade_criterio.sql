-- gravidade_criterio =
--   fator_risco · fator_tempo
--   = ( (risco_evento_gatilho + amortecedor_risco) / (risco_maximo_escala + amortecedor_risco) )
--   · ( dias_atraso / intervalo_urgencia )

{% macro monitora_cancer_gravidade_criterio(
    dias_atraso,
    intervalo_urgencia,
    risco_evento_gatilho,
    amortecedor_risco,
    risco_maximo_escala,
    risco_padrao_quando_nulo=2
) %}
  {{ monitora_cancer_fator_risco(
      risco_evento_gatilho,
      amortecedor_risco,
      risco_maximo_escala,
      risco_padrao_quando_nulo
  ) }} * (
    {{ dias_atraso }} / {{ intervalo_urgencia }}
  )
{% endmacro %}

/*
    Gravidade de um critério ativo do score de monitora_cancer (fórmula
    no header do arquivo). Combina:
      • fator_risco — macro monitora_cancer_fator_risco.
      • fator_tempo — dias_atraso / intervalo_urgencia (linear, sem cap;
        a cada intervalo_urgencia dias adicionais, cresce em 1).

    dias_atraso é calculado fora da macro (monitora_cancer_dias_atraso) e
    só é > 0 quando gatilho disparou e desfecho esperado ainda não chegou
    — eventos pós-desfecho são filtrados antes da chamada.

    Retorna FLOAT64. Referência: OECD/JRC Handbook on Constructing
    Composite Indicators (Nardo et al., 2008), §5.4.
*/
