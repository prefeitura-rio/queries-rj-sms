# Queries SMS/RJ
- Administrador: ([Thiago Trabach](https://github.com/ThiagoTrabach))

## Setup do Projeto
- Instale as dependências com `pip install -r requirements.txt`.
- Obtenha o arquivo de credenciais `credentials.json`.
- No arquivo `./profiles.yml`, defina onde o arquivo de credenciais estará.
- Dê privilegio de execução para o script `./recce.sh`
  - **Linux**: `chmod +x recce.sh`
  - **Windows**:
  - **MacOS**: 
- Rode `dbt deps` para instalar os pacotes de dependência do projeto

## Usando
- Uso Comum
  - Construção de Modelos: `dbt run`
  - Teste de Modelos: `dbt test`
  - Gere a documentação: `dbt docs`
- Dica: no `run` ou no `test`, utilize `--select` para filtrar modelos
  - Por nome do modelo: `dbt run --select <NOME_MODELO>`
  - Por tag: `dbt run --select tag:<NOME_TAG>`

## Usando o Recce
- O Recce permite comparar os dados de produção com os gerados por uma nova configuração DBT
- Para subir um servidor Recce basta chamar o script dando como entrada o nome da branch.
  - **Linux**: Rode `./recce.sh <NOME_BRANCH>`
  - **Windows**:
  - **MacOS**:
- O servidor fica disponível em `localhost:8000`
- Exemplo: no linux eu posso rodar `./recce.sh feat/transforming-vitai-database-into-datalake`
