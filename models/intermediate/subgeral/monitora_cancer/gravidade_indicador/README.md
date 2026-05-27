# Indicador de gravidade 

## O que é

Um número que ordena pacientes em monitoramento de câncer de mama por
**urgência de contato**. Quanto maior o número, mais urgente é a
paciente ser contatada pela equipe.

## Para que serve

A equipe não consegue ligar para todas as pacientes no mesmo dia.
Precisa de uma fila. Este indicador é essa fila - junta vários sinais
clínicos em um único número (`gravidade_total`) fácil de ordenar.

## A intuição

O `gravidade_total` sobe quando, para uma paciente:

- **Há mais pendências em aberto.** Pendência = uma "tarefa esperando
  ser feita". Por exemplo, biópsia pedida mas não realizada.
- **As pendências estão paradas há mais tempo.** Cada tipo de pendência
  tem uma folga tolerável definida pela equipe clínica. Passou da
  folga, começa a pesar.
- **A pendência é clinicamente mais grave.** Um diagnóstico já
  confirmado pesa mais que uma suspeita em rastreio.
- **A paciente está gestante.** Atendimento prioritário.

## Termos do pseudocódigo

(explico antes de usar - todos significam algo simples; nome em
parênteses é o que aparece no SQL)

- **Critério** (= "pendência" da intuição acima): um motivo registrado
  para a paciente precisar de atenção. Uma paciente pode ter vários
  critérios ativos ao mesmo tempo.
- **`dias_atraso`**: dias passados desde o início do critério,
  descontando a folga tolerável.
- **`risco_evento_gatilho`**: gravidade clínica do caso, de 1 (leve)
  a 4 (grave). Vem dos exames e da regulação.
- **`peso_criterio`**: importância relativa do tipo de critério.
  "Diagnóstico confirmado" pesa mais que "suspeita em rastreio".
- **`gravidade_criterio`** = `risco_evento_gatilho × dias_atraso` -
  o quão "pesado" o critério está ficando.
- **`contribuicao_criterio`** = `peso_criterio × gravidade_criterio` -
  contribuição de um critério para o `gravidade_total` da paciente.
- **`gravidade_termo_max`**, **`gravidade_termo_soma`**: agregações da
  `contribuicao_criterio` por paciente (a maior e a soma de todas).
- **`peso_carga_total`**: quanto o `gravidade_termo_soma` pesa no
  `gravidade_total` (controla quanto "ter várias pendências" importa).
- **`multiplicador_gestante`**: incremento aplicado ao
  `gravidade_termo_max` quando a paciente é gestante.

## Pseudocódigo

```text
PARA CADA paciente:

    PARA CADA critério ativo da paciente:
        gravidade_criterio    = risco_evento_gatilho × dias_atraso
        contribuicao_criterio = peso_criterio × gravidade_criterio

    gravidade_termo_max  = a MAIOR contribuicao_criterio entre os critérios
    gravidade_termo_soma = a SOMA das contribuicao_criterio

    SE a paciente é gestante:
        multiplicador_gestante = 1     -- valor atual; configurável
    SENÃO:
        multiplicador_gestante = 0

    gravidade_total = gravidade_termo_max × (1 + multiplicador_gestante)
                    + peso_carga_total × gravidade_termo_soma

    RETORNA gravidade_total
```

**O que isso significa na prática:** com `multiplicador_gestante = 1`
(valor atual), o `gravidade_termo_max` da gestante é multiplicado por
`(1 + 1) = 2` - ou seja, **dobra**. Para não-gestante, multiplica por
`(1 + 0) = 1` - sem efeito.

**Por que mexer só no `gravidade_termo_max`?** Gestante com critério
grave sobe materialmente na fila, mas a "carga" de pendências adicionais
(via `gravidade_termo_soma`) conta igual para todas. Gestante sem nenhum
critério ativo continua com `gravidade_total = 0` (não há
`gravidade_termo_max` para o `multiplicador_gestante` atuar).

**Por que `gravidade_termo_max` E `gravidade_termo_soma`?** Combinar os
dois garante que tanto um único caso muito grave quanto uma carga
acumulada de casos médios fazem a paciente subir na fila. Sem isso, ou
só os casos catastróficos importariam, ou só o volume de pendências.

## Critérios atuais e seus parâmetros

Cada critério tem dois parâmetros ajustáveis:

- **`intervalo_urgencia_dias`** (a "folga tolerável" da intuição): dias que
  o critério pode ficar parado antes de começar a pesar no `gravidade_total`.
- **`peso_criterio`**: importância relativa do critério no score. Calibrado
  em 2026-05 para refletir a hierarquia clínica
  *diagnóstico confirmado > SER em curso > rastreio*.

Sistemas mencionados nos critérios:

- **SISCAN**: laudos de exames de mama (mamografia, biópsia).
- **SISREG**: regulação ambulatorial municipal.
- **SER**: regulação estadual para oncologia (porta de entrada da UNACON).

| # | Critério (nome no SQL) | Quando dispara | `intervalo_urgencia_dias` | `peso_criterio` |
|---|---|---|---:|---:|
| C1 | `SISCAN_MAMA_CAT_0_4_5__SISREG_ULTRA_OU_BIOPSIA` | Mamografia Categoria 0/4/5 (suspeita) sem ultra ou biópsia no SISREG depois | 10 | 1.0 |
| C2 | `SISCAN_MAMA_CAT_6__SER` | Mamografia Categoria 6 (diagnóstico) sem evento no SER depois | 5 | 3.0 |
| C3 | `SISCAN_BIOPSIA_NEOPLASICA__SER` | Biópsia com lesão neoplásica sem evento no SER depois | 5 | 3.0 |
| C4 | `SISREG_BIOPSIA_PROGRESSO` | Biópsia no SISREG com autorização ou execução parada | 20 (por etapa) | 1.0 |
| C5 | `SER_PENDENTE__STATUS_UPDATE` | Solicitação SER travada no status "PENDENTE" | 10 | 2.0 |
| C6 | `SER_EM_FILA__STATUS_UPDATE` | Solicitação SER travada no status "EM_FILA" | 60 | 2.0 |
| C7 | `SER_FALHA__NOVA_SER` | SER cancelada/não-confirmada sem nova solicitação SER | 10 | 2.0 |

## Onde ajustar os parâmetros

- **Pesos clínicos** (qual tipo de critério pesa mais): macro
  `monitora_cancer_pesos_clinicos`.
- **`peso_carga_total`, `multiplicador_gestante`**: topo de
  `int_monitora_cancer__gravidade.sql`.
- **Folgas e gatilhos de cada critério**: topo de
  `int_monitora_cancer__gravidade_instancias.sql`.

## Onde a lógica vive

| Arquivo | Para que serve |
|---|---|
| `int_monitora_cancer__gravidade_instancias.sql` | calcula os critérios ativos (1 linha por critério) |
| `int_monitora_cancer__gravidade.sql` | agrega os critérios num `gravidade_total` por paciente |
| `mart_monitora_cancer__gravidade.sql` | tabela final que alimenta a fila de contato |
| `mart_monitora_cancer__gravidade_instancias.sql` | tabela de critérios, usada para análises de calibração |
