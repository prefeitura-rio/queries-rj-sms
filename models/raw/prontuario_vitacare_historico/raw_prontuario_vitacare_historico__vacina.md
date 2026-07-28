# raw_prontuario_vitacare_historico__vacina

## Visão geral
Modelo contendo dados históricos de vacinações da atenção primária. Registros de vacinação
vindos do backup histórico do Vitacare — usado como fonte complementar para cobrir gaps
que eventualmente não cheguem pela API. Como a API só começou a ser enviada recentemente e 
cobre apenas o dia atual pra frente, esse histórico é uma das fontes que temos para tudo 
que aconteceu antes disso.

## Fonte de dados
Google Drive.
- **Natureza do arquivo:** Cumulativo — o backup de cada mês contém todo o histórico 
- do backup anterior + os dados novos do mês. Não é um envio incremental/delta.
- **Caminho até o dbt:** arquivo .bak disponibilizado no google drive → gcs como storage
- e persintência → instância sqlserver no cloudsql → gcs em parquet → bigquery (staging) → dbt.
- **Frequência:** Mensal.
- **Tabela/de origem:** `brutos_prontuario_vitacare_historico_staging.vacinas`

## Granularidade
Uma linha = um registro de dose aplicada a um paciente


## O que esse modelo faz
Apenas tratamento estrutural: renomeia colunas pra convenção interna, aplica
cast de tipos, concatena chaves pra formar identificadores globais. 
Nenhuma lógica de tratamento avançado aqui 

## Decisões e particularidades conhecidas
- Datas com valor <= 1900-01-01 são bug conhecido da fonte (ausência de data
  real) — tratadas como inválidas já nesta camada.
- **Preservação de proveniência (Metadados):** Como o arquivo de origem é cumulativo, 
- cada novo backup mensal traz todos os registros antigos novamente. Para evitar que a 
- data original de ingestão seja perdida em cada atualização, utilizamos a estratégia 
- incremental com `merge_exclude_columns` no dbt. Isso garante que a coluna `loaded_at` 
- represente fielmente a data da *primeira vez* que o registro entrou no Data Lake, 
- sem ser sobrescrita pelas cargas subsequentes.

## Dependências
- **Upstream:** brutos_prontuario_vitacare_historico_staging.vacinas
- **Downstream:** intermediario_vitacare_historico.vacinas
