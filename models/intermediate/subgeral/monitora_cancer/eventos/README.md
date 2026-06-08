# eventos/

Compute compartilhado por paciente.

| Modelo | Papel |
|---|---|
| `int_monitora_cancer__eventos_episodios` | Tabela compartilhada por `mart__gravidade` e `mart__pacientes_linha_tempo` (materializada para evitar trabalho duplo). Calcula o `run_id` (episódio de cuidado, gap ≤ 180 dias) e os tempos por paciente broadcast em cada linha: `tempo_total` e, no run atual, `tempo_diagnostico` e `tempo_diagnostico_sem_tratamento`. |
| `int_monitora_cancer__pendencias` | Deriva rótulos de pendência por paciente a partir de `eventos_episodios`. |
