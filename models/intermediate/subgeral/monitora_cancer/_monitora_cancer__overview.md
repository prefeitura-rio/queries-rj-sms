{% docs monitora_cancer__overview %}
Monitoramento da linha de cuidado de **câncer de mama** na rede municipal
do Rio de Janeiro (SMS/SUBGERAL/CR/NTI). Integra três sistemas de origem
(SISCAN, SISREG, SER), reconstrói a jornada de cada paciente e produz uma
fila de urgência de contato (score de gravidade) e uma linha do tempo por
paciente. Atualização diária. Visão geral, DAG e glossário de projeto em
`models/intermediate/subgeral/monitora_cancer/README.md`.
{% enddocs %}


{% docs monitora_cancer__episodio_run %}
Episódio (ou "run"): sequência consecutiva de eventos da mesma paciente
cujos gaps de `data_referencia_evento` são ≤ 180 dias
(`episodio_gap_dias`). O `run_id` incrementa a cada gap maior que isso
dentro do mesmo paciente. O score de gravidade e a linha do tempo
consideram apenas o **run atual** (último episódio de cuidado).
{% enddocs %}


{% docs monitora_cancer__status %}
Classificação da paciente no monitoramento:
"UNACON" quando há pelo menos um evento vindo do SER (regulação para
oncologia); "DIAGNOSTICO" quando há evento com critério de diagnóstico
confirmado; "SUSPEITA" nos demais casos da população-alvo.
{% enddocs %}


{% docs monitora_cancer__gravidade_score %}
Apresentação 0-100 do score de gravidade da paciente. Reflete
`mart_monitora_cancer__gravidade.gravidade_total_0_100`, com clip no p95
dinâmico de `gravidade_total` do run atual — acima do teto, satura em 100.
A ordenação canônica da fila continua via `gravidade_total` bruto (em
`mart_monitora_cancer__gravidade`), não esta coluna. Pacientes sem critério
ativo recebem 0 — inclusive gestantes (o multiplicador atua sobre o
termo_max e não cria score onde não há critério). Fórmula completa,
pseudocódigo e exemplo numérico em
`models/intermediate/subgeral/monitora_cancer/gravidade_indicador/README.md`.
{% enddocs %}
