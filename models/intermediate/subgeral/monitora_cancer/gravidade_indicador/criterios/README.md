# criterios/

Um arquivo SQL por critério de gravidade. Cada arquivo é **ephemeral**, lê
[`int_monitora_cancer__eventos_run_atual`](../int_monitora_cancer__eventos_run_atual.sql)
(fonte compartilhada dos eventos do run atual), define as CTEs locais que
precisar e termina retornando a **relação canônica bruta** de 8 colunas:

```
(cpf_particao, criterio, etapa, data_trigger, dias_atraso,
 intervalo_urgencia_dias, risco_evento_gatilho, peso_criterio)
```

O agregador
[`int_monitora_cancer__gravidade_instancias`](../int_monitora_cancer__gravidade_instancias.sql)
consome todos via `UNION ALL`, aplica a fórmula (`fator_tempo`,
`fator_risco`, `gravidade_criterio`), faz o JOIN de gestante e o filtro
`dias_atraso > 0`.

## Famílias

| Critério | Arquivo | Família | Macro |
|---|---|---|---|
| C1 | `..._criterio_1_siscan_cat045__sisreg` | cross-evento | `monitora_cancer_criterio_cross_evento` |
| C2 | `..._criterio_2_siscan_cat6__ser` | cross-evento | `monitora_cancer_criterio_cross_evento` |
| C3 | `..._criterio_3_biopsia_neopl__ser` | cross-evento | `monitora_cancer_criterio_cross_evento` |
| C4 | `..._criterio_4_sisreg_biopsia__intra` | intra-evento (2 legs) | `monitora_cancer_criterio_intra_evento` |
| C5 | `..._criterio_5_ser_pendente` | intra-status | `monitora_cancer_criterio_intra_evento` |
| C6 | `..._criterio_6_ser_em_fila` | intra-status | `monitora_cancer_criterio_intra_evento` |
| C7 | `..._criterio_7_ser_falha__nova_ser` | cross-evento (estrito `>`) | `monitora_cancer_criterio_cross_evento` |

- **cross-evento**: define CTEs `criterio_N_triggers` e
  `criterio_N_desfecho_esperado`; a macro faz o anti-join `NOT EXISTS`.
- **intra-evento / intra-status**: sem CTEs; a macro lê direto de
  `eventos_run_atual` (via `source_cte_name=ref(...)`) e a desativação está
  no `source_filter`.

> **Nomes de CTE únicos por arquivo.** Como os critérios são ephemeral e o
> dbt faz *hoisting* dos CTEs para o agregador, CTEs de arquivos diferentes
> não podem colidir. Por isso os desfechos dos critérios 2 e 3 (ambos
> "solicitação SER") são CTEs separadas `criterio_2_desfecho_esperado` e
> `criterio_3_desfecho_esperado`, não uma compartilhada.

## Para adicionar um critério novo

1. Copiar o arquivo de um critério da mesma família (cross-evento ou
   intra-evento).
2. Ajustar gatilho/desfecho (ou `source_filter`), o intervalo
   (`{% set criterio_N_intervalo %}` no topo) e o peso
   (`monitora_cancer_pesos_clinicos()[N-1]` — adicionar o peso na macro
   `monitora_cancer_pesos_clinicos`).
3. Garantir nomes de CTE únicos (prefixados por `criterio_N_`).
4. Adicionar o `ref()` no `UNION ALL` de
   `int_monitora_cancer__gravidade_instancias.sql`.
5. Atualizar o `accepted_values` do teste `criterio` em
   `_gravidade_indicador__schema.yml` (camada mart).
