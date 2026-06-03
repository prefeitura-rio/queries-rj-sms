# Sobre o Monitora Câncer

## 1. Introdução

O Monitora Câncer é um sistema de acompanhamento da linha de cuidado do câncer de mama na rede municipal de saúde do Rio de Janeiro. Ele cruza informações de três sistemas oficiais para reconstruir a jornada de cada pessoa do sexo feminino em investigação ou tratamento e ajuda a equipe a priorizar os contatos do dia.

Este é um produto do **Núcleo de Tecnologia da Informação (NTI)**, vinculado à **Central de Regulação** da **Subsecretaria Geral (SUBGERAL)** da Secretaria Municipal de Saúde do Rio de Janeiro (SMS Rio).

O sistema entrega duas coisas principais:

- uma **fila de urgência de contato**, ordenada por um número (chamado *score de gravidade*) que indica quão urgente é ligar para cada pessoa;
- uma **linha do tempo por pessoa**, com todos os eventos registrados e as pendências em aberto.

```mermaid
flowchart LR
    A[SISCAN<br/>laudos<br/>de exames]
    B[SISREG<br/>regulação<br/>municipal]
    C[SER<br/>regulação<br/>estadual]
    M[Monitora<br/>Câncer]
    D[Fila de<br/>Urgência<br/>de Contato]
    E[Linha do<br/>Tempo por<br/>Paciente]
    A --> M
    B --> M
    C --> M
    M --> D
    M --> E
```

## 2. Objetivo

As equipes da SMS Rio não conseguem entrar em contato com todas as pessoas em monitoramento no mesmo dia. É preciso uma ferramenta para apoiar as decisões neste processo.

O objetivo do Monitora Câncer é, portando, responder, todos os dias, a duas perguntas:

1. **Quais pessoas estão em monitoramento agora?**
2. **Em qual ordem a equipe deve contatá-las?**

A resposta da primeira pergunta sai como uma lista de pessoas com seus dados de contato. A resposta da segunda pergunta sai como um número de 0 a 100 para cada pessoa: quanto maior, mais urgente.

## 3. Fontes de Dados

O sistema cruza três fontes oficiais:

- **SISCAN** (Sistema de Informação do Câncer): registra os laudos de exames de mama, como mamografias e biópsias. Traz o resultado clínico, incluindo a categoria BI-RADS - uma classificação padronizada que vai de 0 a 6 e indica o grau de suspeita do exame.
- **SISREG** (Sistema de Regulação adotado no Município): registra os pedidos de exames e consultas da Atenção Primária à Saúde (APS) da rede municipal do Rio. No Monitora Câncer, são utilizadas as datas de solicitação, autorização e execução de cada procedimento.
- **SER** (Sistema Estadual de Regulação): registra os encaminhamentos para a alta complexidade oncológica, ou seja, para as **UNACON** (Unidades de Alta Complexidade em Oncologia). É a porta de entrada para o tratamento de câncer.

Cada exame, consulta ou encaminhamento vira uma linha no sistema (um evento). As três fontes alimentam a mesma base de eventos, e o restante da lógica trabalha em cima dela.

## 4. Lógica de Seleção de Pacientes e de Classificação

### Como uma pessoa entra no monitoramento

Uma pessoa entra na lista quando atende, ao mesmo tempo, a todos os critérios abaixo:

- é do sexo feminino (registro cadastral);
- está viva (sem registro de óbito);
- tem pelo menos um evento registrado a partir de **01/01/2025**;
- esse evento é considerado de **suspeita** ou de **diagnóstico** de câncer de mama (veja a Seção 5).

### Status da pessoa

Quando uma pessoa entra, ela também recebe um **status**. O status diz em que ponto da linha de cuidado ela está:

- **SUSPEITA**: há indícios em exames ou pedidos de exames, mas o câncer ainda não foi confirmado.
- **DIAGNÓSTICO**: o câncer foi confirmado por exame (por exemplo, mamografia BI-RADS categoria 6 ou biópsia com lesão neoplásica).
- **UNACON**: a pessoa já foi encaminhada para uma unidade de alta complexidade oncológica - ou seja, tem pelo menos um evento registrado no SER.

A regra de decisão é a seguinte:

```mermaid
flowchart TD
    A[Paciente na<br/>população-alvo]
    Q1{Tem pelo<br/>menos um<br/>evento<br/>no SER?}
    Q2{Algum exame<br/>com<br/>diagnóstico<br/>confirmado?}
    R1[UNACON]
    R2[DIAGNÓSTICO]
    R3[SUSPEITA]
    A --> Q1
    Q1 -- sim --> R1
    Q1 -- não --> Q2
    Q2 -- sim --> R2
    Q2 -- não --> R3
```

**Exemplo:** Uma pessoa faz mamografia e o laudo sai como BI-RADS 4. Ela entra com status SUSPEITA. Em seguida, faz uma biópsia que confirma lesão neoplásica: passa para DIAGNÓSTICO. Quando o pedido de consulta com mastologista oncológico aparece no SER, passa para UNACON.

## 5. Tabela de Procedimentos Monitorados

Cada procedimento abaixo é considerado de interesse para o monitoramento. As duas últimas colunas indicam se a presença daquele procedimento, sozinha, já sinaliza suspeita ou diagnóstico de câncer de mama.

### SISREG (regulação municipal)

| Código  | Procedimento                  | Sinaliza Suspeita | Sinaliza Diagnóstico |
|---------|-------------------------------|:-----------------:|:--------------------:|
| 703716  | Mamografia de Rastreio        | não               | não                  |
| 2018735 | Mamografia Diagnóstica        | sim               | não                  |
| 701867  | Consulta em Mastologia        | não               | não                  |
| 2300036 | Consulta em Mastologia        | não               | não                  |
| 3100093 | RNM de Mamas                  | não               | não                  |
| 3105274 | RNM de Mama Esquerda          | não               | não                  |
| 3105275 | RNM de Mama Direita           | não               | não                  |
| 1407035 | USG de Mamas                  | não               | não                  |
| 1670021 | USG de Mamas                  | não               | não                  |
| 228009  | USG de Mamas                  | não               | não                  |
| 225039  | USG de Mamas                  | não               | não                  |
| 820029  | USG de Mamas - para Biópsia   | sim               | não                  |
| 2018205 | Biópsia                       | sim               | não                  |
| 816013  | Biópsia - USG                 | sim               | não                  |
| 820058  | Biópsia - MMG                 | sim               | não                  |

### SER (regulação estadual)

| Código | Procedimento                       | Sinaliza Suspeita | Sinaliza Diagnóstico |
|--------|------------------------------------|:-----------------:|:--------------------:|
| 560    | Consulta em Mastologia             | não               | não                  |
| 1035   | Consulta em Mastologia (Oncologia) | não               | sim                  |
| 1049   | Consulta em Mastologia (Oncologia) | não               | sim                  |

### SISCAN (laudos)

No SISCAN, a sinalização não depende do procedimento, mas do resultado do laudo:

- mamografia categoria 0, 4 ou 5 sinaliza **suspeita**;
- mamografia categoria 6 sinaliza **diagnóstico**;
- biópsia com lesão neoplásica sinaliza **diagnóstico**.

Resumo das categorias BI-RADS: 0 = inconclusiva; 1 e 2 = sem sinais suspeitos; 3 = provavelmente benigna; 4 e 5 = suspeita; 6 = malignidade conhecida.

## 6. Critérios de Exclusão

A pessoa sai da lista quando uma das regras abaixo se aplica.

| Regra                             | Quando se aplica |
|-----------------------------------|------------------|
| Óbito                             | Quando há registro cadastral de óbito da pessoa. |
| Mamografia BI-RADS 1 ou 2         | Quando o último evento da pessoa foi uma mamografia com resultado em categoria 1 ou 2 (sem indícios de câncer). |
| Duas mamografias BI-RADS 3        | Quando os dois últimos eventos da pessoa foram mamografias em categoria 3 (provavelmente benignas). |
| Biópsia sem lesão                 | Quando o último evento foi uma biópsia sem lesão neoplásica nem benigna identificada (descarta o achado). |
| SER antigo                        | Quando o último evento foi um encaminhamento no SER já finalizado há tempo suficiente (veja parâmetros na Seção 8). |

**Observação importante:** As regras de mamografia BI-RADS 1 ou 2, biópsia sem lesão e SER antigo se aplicam apenas ao **último** evento da pessoa. Se mais tarde aparecer um evento novo (por exemplo, uma nova mamografia suspeita), a pessoa pode voltar para a lista automaticamente.

## 7. Procedimentos Monitorados: Visão da Jornada

Os procedimentos da Seção 5 representam etapas típicas da jornada da pessoa. Eles podem ser agrupados pela finalidade clínica:

- **Rastreio**: mamografia de rastreio. Identifica indícios em pessoas sem queixa.
- **Investigação de suspeita**: mamografia diagnóstica, USG de mamas, RNM de mamas. Detalham um achado.
- **Confirmação diagnóstica**: biópsia (em vários tipos). Confirma se a lesão é câncer.
- **Acompanhamento ambulatorial**: consulta em mastologia. Direciona os próximos passos.
- **Encaminhamento oncológico**: consulta em mastologia oncológica no SER. Porta de entrada da UNACON.

Uma jornada típica atravessa essas etapas mais ou menos nesta ordem:

```mermaid
flowchart LR
    R[Mamografia<br/>de Rastreio]
    I[Mamografia<br/>Diagnóstica<br/>USG / RNM]
    C[Biópsia]
    M[Consulta em<br/>Mastologia]
    O[Consulta<br/>Oncológica<br/>no SER]
    U[UNACON]
    R --> I
    I --> C
    I --> M
    C --> M
    M --> O
    O --> U
```

O sistema acompanha a pessoa em todas essas etapas e dispara alertas quando o tempo entre uma etapa e outra ultrapassa os limites esperados.

## 8. Parâmetros de Tempo

O sistema usa vários parâmetros de tempo. Eles estão consolidados nas tabelas a seguir.

### 8.1 Limites de regulação por procedimento

Para cada procedimento, o sistema espera que ele seja autorizado e executado dentro de um prazo. O prazo total é a soma de duas partes: o tempo entre solicitação e autorização, e o tempo entre autorização e execução. Todos os valores estão em dias.

| Fonte  | Procedimento                       | Solicitação até Autorização | Autorização até Execução | Total |
|--------|------------------------------------|:---------------------------:|:------------------------:|:-----:|
| SISREG | Mamografia de Rastreio             | 0                           | 50                       | 50    |
| SISREG | Mamografia Diagnóstica             | 5                           | 15                       | 20    |
| SISREG | Consulta em Mastologia             | 15                          | 45                       | 60    |
| SISREG | RNM de Mamas (todas)               | 5                           | 5                        | 10    |
| SISREG | USG de Mamas                       | 15                          | 45                       | 60    |
| SISREG | USG de Mamas - para Biópsia        | 5                           | 15                       | 20    |
| SISREG | Biópsia (todos os tipos)           | 5                           | 15                       | 20    |
| SER    | Consulta em Mastologia             | 0                           | 60                       | 60    |
| SER    | Consulta em Mastologia (Oncologia) | 0                           | 60                       | 60    |

### 8.2 Outros parâmetros de tempo do projeto

| Parâmetro                                  | Valor       | Para que serve |
|--------------------------------------------|-------------|----------------|
| Data de corte                              | 01/01/2025  | Eventos anteriores a essa data não contam para entrada da pessoa na lista. |
| Episódio de cuidado                        | 180 dias    | Eventos da mesma pessoa separados por mais de 180 dias são tratados como episódios diferentes de cuidado. O sistema considera apenas o episódio mais recente. |
| Exclusão por SER antigo (status ALTA)      | 3 meses     | Se o último evento da pessoa foi um SER com status ALTA há 3 meses ou mais, a pessoa sai da lista. |
| Exclusão por SER antigo (chegada/cancelada)| 6 meses     | Se o último evento foi um SER com status CHEGADA_CONFIRMADA ou CANCELADA há 6 meses ou mais, a pessoa sai da lista. |
| Alerta de prazo legal próximo do limite    | 45 dias     | A partir de 45 dias desde o diagnóstico, o sistema avisa que o prazo legal para início de tratamento está próximo. |
| Prazo legal para início do tratamento      | 60 dias     | Após o diagnóstico de câncer, a pessoa tem direito a iniciar o tratamento em até 60 dias. |

### 8.3 Folgas dos critérios do score

Cada critério do score tem uma **folga tolerável** - um número de dias durante o qual a pendência ainda não pesa no score. A lista completa está na Seção 10.

A folga não é um valor arbitrário: ela representa o **tempo dentro do qual se espera que aquele desfecho aconteça** (por exemplo, o prazo razoável para uma biópsia solicitada ser realizada, ou para um encaminhamento ao SER ser autorizado). Enquanto a pendência está dentro desse prazo, ela segue o curso normal e não exige atenção prioritária. Por isso o score só passa a pesar **depois** que a folga é ultrapassada: o objetivo é destacar quem já passou do tempo em que deveria ter recebido a intervenção - ou seja, quem precisa de atenção prioritária.

## 9. Score de Gravidade

### O que é

O score de gravidade é um número de 0 a 100 que ordena as pessoas por **urgência de contato**. Quanto maior o número, mais urgente a pessoa precisa ser contatada pela equipe.

### A intuição

O score sobe quando, para uma pessoa, acontece uma combinação de fatores como:

- **Há mais pendências em aberto:** Uma pendência é uma tarefa esperando ser feita - por exemplo, biópsia pedida mas ainda não realizada.
- **As pendências estão paradas há mais tempo:** Cada tipo de pendência tem uma folga tolerável. Passou da folga, começa a pesar.
- **A pendência é clinicamente mais grave:** Um diagnóstico confirmado pesa mais do que uma suspeita em rastreio.
- **A pessoa está gestante:** Recebe atendimento prioritário.

### Exemplo passo a passo

Imagine a pessoa Maria. Ela tem duas pendências ativas e está gestante.

- **Pendência A** - mamografia categoria 6 (diagnóstico confirmado) sem encaminhamento ao SER. Folga: 5 dias. Atraso: 10 dias. Peso clínico: alto.
- **Pendência B** - solicitação no SER parada no status "pendente". Folga: 10 dias. Atraso: 10 dias. Peso clínico: médio.

A pendência A pesa muito mais, porque (1) o atraso já é o dobro da folga, (2) o risco clínico é máximo (categoria 6) e (3) o peso clínico do tipo de pendência é alto. A pendência B contribui menos: o atraso só igualou a folga e o peso é menor.

Como a Maria é gestante, a contribuição da pendência mais grave (A) **dobra**. A pendência B continua contribuindo, em menor escala.

Resultado: a Maria fica entre as primeiras da fila do dia, acima de pessoas não gestantes com pendência parecida.

### Como interpretar o score na prática

- **O score serve para ordenar a fila do dia, não para julgar o caso clínico.** Score 80 não significa "doença mais grave que score 50" - significa "a equipe deve ligar antes para esta pessoa".
- **Use o score como prioridade, não como diagnóstico.** O cuidado clínico continua sendo avaliado caso a caso.
- **Score 0 não significa "pessoa sem problema".** Significa "sem pendência em aberto nesta data". A pessoa continua sendo monitorada e pode subir na fila no dia seguinte.
- **Não compare scores entre dias como se fosse evolução clínica.** O sistema recalcula a escala 0-100 todos os dias com base na distribuição daquele dia. Use o score sempre como **posição na fila daquele dia**.
- **Pessoas gestantes sem pendência em aberto ficam com score 0.** O multiplicador de gestante só atua quando há pendência ativa.

## 10. Critérios e Parâmetros do Score

O score combina sete critérios. Cada um tem uma folga tolerável (intervalo de urgência) e um peso clínico.

| #  | Critério                                                                  | Folga (dias)   | Peso |
|----|---------------------------------------------------------------------------|:--------------:|:----:|
| C1 | Mamografia categoria 0, 4 ou 5 (suspeita) sem ultrassom ou biópsia depois | 10             | 1.0  |
| C2 | Mamografia categoria 6 (diagnóstico) sem solicitação ao SER depois        | 5              | 3.0  |
| C3 | Biópsia com lesão neoplásica sem solicitação ao SER depois                | 5              | 3.0  |
| C4 | Biópsia no SISREG com autorização ou execução parada                      | 20 por etapa   | 1.0  |
| C5 | Solicitação no SER travada no status "Pendente"                           | 10             | 2.0  |
| C6 | Solicitação no SER travada no status "Em Fila"                            | 60             | 2.0  |
| C7 | SER cancelada ou não confirmada sem nova solicitação SER                  | 10             | 2.0  |

A calibração dos pesos reflete a hierarquia clínica: **diagnóstico confirmado** (C2, C3) pesa mais do que **encaminhamento em curso** (C5, C6, C7), que pesa mais do que **rastreio em investigação** (C1, C4).

A definição matemática completa - como esses sete critérios viram um único número de 0 a 100, com a fórmula passo a passo e os parâmetros $\lambda$ (peso de carga total), $\alpha$ (multiplicador de gestante) e o amortecimento de risco - está no **Anexo A**, ao final do documento. Para usar o Monitora Câncer no dia a dia, as Seções 9 e 10 bastam; o Anexo serve a quem precisa auditar ou reproduzir o cálculo exato.

## 11. Pendências

Pendência é qualquer tarefa clínica esperando ser feita. O sistema mostra as pendências em aberto de cada pessoa na linha do tempo. As pendências se dividem em três famílias.

### Pendências do SISREG (quando o último evento é do SISREG)

| Pendência                       | Quando ativa |
|---------------------------------|--------------|
| Pendente de autorização SISREG  | Procedimento foi solicitado mas ainda não foi autorizado. |
| Pendente de realização SISREG   | Procedimento foi autorizado mas ainda não foi executado. |
| Aguardando execução SISREG      | Procedimento tem data de execução agendada no futuro. |
| Devolvido SISREG                | Status do procedimento indica devolução. |
| Falta SISREG                    | Status do procedimento indica falta da pessoa. |

### Pendências do SER (quando o último evento é do SER)

| Pendência                       | Quando ativa |
|---------------------------------|--------------|
| Pendente de autorização SER     | Solicitação no SER ainda não foi autorizada. |
| Pendente de realização SER      | Solicitação foi autorizada mas ainda não foi executada. |
| Aguardando execução SER         | Solicitação tem data de execução agendada no futuro. |
| Procedimento cancelado SER      | Solicitação no SER foi cancelada. |
| Falta SER                       | Pessoa não confirmou a chegada (status CHEGADA_NAO_CONFIRMADA). |

### Pendências de encaminhamento para UNACON (somente para status DIAGNÓSTICO)

Estas três pendências descrevem a mesma situação em níveis de urgência crescentes. O sistema mostra apenas a mais grave delas.

| Pendência                                                | Quando ativa |
|----------------------------------------------------------|--------------|
| Pendente de solicitação para UNACON                      | Pessoa diagnosticada há menos de 45 dias e ainda sem solicitação SER. |
| Prazo para início de tratamento próximo do limite legal  | Pessoa diagnosticada há 45 dias ou mais e ainda sem solicitação SER. |
| Prazo legal para início de tratamento ultrapassado       | Pessoa diagnosticada há 60 dias ou mais e ainda sem solicitação SER (prazo legal de 60 dias ultrapassado). |

## 12. A Linha do Tempo da Pessoa

Além da posição na fila (o score de gravidade), o sistema monta, para cada pessoa, uma ficha que a equipe usa no contato. Ela reúne:

- **Identificação e contato**: nome, idade, raça/cor, telefone da pessoa e os telefones da clínica da família e da equipe responsável.
- **Situação atual**: o status (SUSPEITA, DIAGNÓSTICO ou UNACON, veja a Seção 4) e o score de gravidade (Seção 9).
- **Histórico de eventos**: todos os exames, consultas e encaminhamentos, do mais antigo ao mais recente. Cada evento traz o procedimento, as datas (pedido, autorização, realização e resultado), as unidades envolvidas, o resultado e o nível de risco informado pelo sistema de origem (por exemplo, a categoria BI-RADS no SISCAN ou a prioridade no SER). Aparece também um aviso quando a etapa está demorando mais do que o esperado (prazos na Seção 8).
- **Pendências em aberto** (Seção 11).
- **Tempos da linha de cuidado**, medidos no episódio atual (a sequência mais recente de eventos, com intervalos de até 180 dias - Seção 8.2):
  - **Tempo total** (`tempo_total`): dias do início do episódio até a entrada na UNACON; se a pessoa ainda não chegou, conta até hoje.
  - **Tempo até o diagnóstico** (`tempo_diagnostico`): dias do início do episódio até a pessoa passar a DIAGNÓSTICO ou UNACON.
  - **Tempo diagnosticada sem tratamento** (`tempo_diagnostico_sem_tratamento`): dias entre o diagnóstico e o encaminhamento ao SER, acompanhando o prazo legal de 60 dias (Seção 8.2).

Assim, em uma única tela, a equipe vê quem é a pessoa, em que ponto da linha de cuidado ela está e o que falta fazer.

## 13. Atualização de Dados

- **Frequência**: o sistema é atualizado **uma vez por dia**, de forma automática.
- **O que é recalculado**: a lista de pessoas em monitoramento, o status de cada uma, as pendências em aberto e o score de gravidade.
- **Defasagem possível**: o sistema depende dos dados que as fontes (SISCAN, SISREG, SER) já registraram. Se um exame foi realizado mas ainda não foi lançado na sua fonte de origem, ele ainda não aparece aqui.
- **Identificação da execução**: cada atualização deixa uma marca registrando quando rodou, para auditoria.

## Anexo A - Definição matemática do score

Esta seção detalha o cálculo exato do score descrito nas Seções 9 e 10. Ela é voltada a quem precisa **auditar ou reproduzir** o número e pode ser ignorada por quem só vai usar a fila no dia a dia.

### A fórmula do score

O score é construído em cinco passos. Os parâmetros descritos na tabela ao final desta seção aparecem em destaque na fórmula.

Para cada **critério ativo** $c$ de uma pessoa, usamos quatro quantidades:

- $f_c$ = **folga** (intervalo de urgência) do critério, em dias - coluna *Folga* da tabela de critérios (Seção 10). É o **tempo dentro do qual se espera que aquele desfecho aconteça** (por exemplo, o prazo razoável para uma biópsia ser realizada);
- $a_c$ = **dias de atraso** além da folga: $a_c = \max\left(0,\ \text{dias desde o gatilho} - f_c\right)$. Descontamos $f_c$ justamente para isolar o tempo que passou **depois** do prazo esperado - ou seja, o quanto a pessoa já está atrasada para a intervenção que deveria ter ocorrido. Dentro da folga, $a_c = 0$ e o critério não pesa;
- $r_c$ = **risco** do evento que disparou o critério, na escala de 1 a 4 (quando ausente, vale 2). O maior valor possível dessa escala é $r_{\max} = 4$;
- $w_c$ = **peso clínico** do critério - coluna *Peso* da tabela de critérios (Seção 10).

**1. Gravidade de cada critério** combina o atraso com o risco:

$$g_c \;=\; \underbrace{\frac{a_c}{f_c}}_{\text{fator de tempo}} \;\times\; \underbrace{\frac{r_c + 1}{r_{\max} + 1}}_{\text{fator de risco}}$$

O *fator de tempo* cresce em 1 a cada folga adicional de atraso. O *fator de risco* é o **amortecimento de risco**. O denominador é $r_{\max} + 1$, onde $r_{\max} = 4$ é o maior valor da escala de risco (1 a 4) - logo $r_{\max} + 1 = 4 + 1 = 5$. Dividir por $r_{\max} + 1$ normaliza o fator de risco para no máximo $1$, valor atingido exatamente quando $r_c = r_{\max}$. O $+1$ no numerador evita que o risco mínimo zere o fator: com $r_c = 1$ ele vale $2/5 = 0.4$, e não $0$. Assim o risco máximo ($5/5 = 1.0$) vale $2.5\times$ o risco mínimo ($0.4$).

**2. Contribuição de cada critério** pondera a gravidade pelo peso clínico:

$$\text{contrib}_c \;=\; w_c \times g_c$$

Se um mesmo critério dispara várias vezes para a mesma pessoa, mantém-se apenas a contribuição mais alta.

**3. Agregação por pessoa** resume todos os critérios ativos em dois termos:

$$\text{termo}_{\max} = \max_c \left(w_c \cdot g_c\right) \qquad \text{termo}_{\text{soma}} = \sum_c \left(w_c \cdot g_c\right)$$

O $\text{termo}_{\max}$ é a pendência mais grave (domina o score); o $\text{termo}_{\text{soma}}$ é a carga total de pendências.

**4. Score total** combina os dois termos, com o multiplicador de gestante sobre a pendência mais grave e o peso de carga total sobre a soma:

$$\text{gravidade}_{\text{total}} \;=\; \text{termo}_{\max} \cdot \left(1 + \alpha \cdot \mathbf{1}_{\text{gestante}}\right) \;+\; \lambda \cdot \text{termo}_{\text{soma}}$$

onde $\alpha = 1.0$ é o **multiplicador de gestante**, $\lambda = 0.5$ é o **peso de carga total** e $\mathbf{1}_{\text{gestante}}$ vale $1$ se a pessoa é gestante e $0$ caso contrário (ou seja, gestante dobra o $\text{termo}_{\max}$).

**5. Reescala para 0-100** (apenas para apresentação na fila):

$$\text{score}_{0\text{-}100} \;=\; 100 \times \min\left(\frac{\text{gravidade}_{\text{total}}}{\text{teto}},\ 1\right)$$

onde o $\text{teto}$ é o percentil 95 da distribuição de $\text{gravidade}_{\text{total}}$ daquele dia, recalculado a cada atualização, de modo que cerca de 5% das pessoas saturam em 100.

### Outros parâmetros do score

Os três parâmetros abaixo são exatamente os que aparecem na fórmula acima.

| Parâmetro                | Símbolo na fórmula | Valor             | O que faz |
|--------------------------|:------------------:|-------------------|-----------|
| Peso de carga total      | $\lambda$          | 0.5               | Controla o quanto "ter várias pendências ao mesmo tempo" ($\text{termo}_{\text{soma}}$) pesa no score. |
| Multiplicador de gestante| $\alpha$           | 1.0               | Dobra a contribuição da pendência mais grave ($\text{termo}_{\max}$) quando a pessoa está gestante. |
| Amortecimento de risco   | $\dfrac{r_c + 1}{r_{\max} + 1}$ | (risco + 1) / 5 | Normaliza o risco pelo seu valor máximo: o denominador é $r_{\max} + 1 = 4 + 1 = 5$, onde $r_{\max} = 4$ é o maior valor da escala (não é um 5 arbitrário). Suaviza a diferença entre risco baixo e alto, fazendo o risco máximo (4) valer 2.5 vezes o risco mínimo (1). |

## 14. Equipe

Este projeto é desenvolvido pelo **Núcleo de Tecnologia da Informação (NTI)**, na Central de Regulação da **Subsecretaria Geral (SUBGERAL)** da Secretaria Municipal de Saúde do Rio de Janeiro (SMS Rio).

### Equipe Desenvolvedora (NTI)
- Juliana Paranhos - Coordenadora de TI
- Matheus Miloski - Engenheiro de Dados
- Marcos Chagas - Programador Front-End
- Eduardo Ataíde - Programador Back-End

### Equipe de Saúde (SUBGERAL)
- Fernanda Adães - Subsecretária Geral
- Lucas Galhardo - Assessor Técnico
- Paula Bortolon - Assessora Técnica

### Contribuições Anteriores
- Marina Ferraz - Analista de Dados (NTI)
