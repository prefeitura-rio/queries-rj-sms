{% macro monitora_cancer_pesos_clinicos() %}
  {{ return([1.0, 3.0, 3.0, 1.0, 2.0, 2.0, 2.0]) }}
{% endmacro %}

/*
    Pesos clínicos dos 7 critérios de gravidade do monitora_cancer.

    Lista posicional: índice i (0-based) = peso clínico do critério i+1
    (criterio_1_peso, ..., criterio_7_peso).

    FONTE ÚNICA — consumido por:
      • int_monitora_cancer__gravidade_instancias
            → peso_criterio emitido por linha (uma por instância de critério)

    Valores vigentes (calibração 2026-05, validados via análise de
    sensibilidade contra a distribuição real de pacientes ativos):

      C1 SISCAN_MAMA_CAT_0_4_5__SISREG_ULTRA_OU_BIOPSIA  = 1.0   (rastreio)
      C2 SISCAN_MAMA_CAT_6__SER                          = 3.0   (diagnóstico)
      C3 SISCAN_BIOPSIA_NEOPLASICA__SER                  = 3.0   (diagnóstico)
      C4 SISREG_BIOPSIA_PROGRESSO                        = 1.0   (em curso)
      C5 SER_PENDENTE__STATUS_UPDATE                     = 2.0   (SER pendente)
      C6 SER_EM_FILA__STATUS_UPDATE                      = 2.0   (SER em fila)
      C7 SER_FALHA__NOVA_SER                             = 2.0   (SER falhou)

    Hierarquia clínica: diagnóstico confirmado (C2, C3) > SER em curso
    ou com falha (C5, C6, C7) > rastreio pré-suspeita (C1, C4). Calibrada
    em 2026-05 via análise de sensibilidade: peso 3 para diagnóstico
    inverte a composição do top-100 (de rastreio para diagnóstico).

    Tamanho da lista deve ser exatamente 7 (número de critérios). Adicionar
    um critério novo implica:
      1. Adicionar peso aqui;
      2. Criar CTE criterio_N + UNION ALL em
         int_monitora_cancer__gravidade_instancias.sql;
      3. Adicionar ao accepted_values do teste `criterio` no
         _mart_monitora_cancer__schema.yml.

    Recalibrar quando: (a) a equipe clínica reordenar a hierarquia entre
    critérios; (b) a distribuição de pacientes por critério mudar
    materialmente (novos sistemas-fonte, critérios adicionados); (c) a
    composição do top-K (rastreio vs. diagnóstico) sair do esperado.
*/
