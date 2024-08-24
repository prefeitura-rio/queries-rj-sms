# Queries SMS/RJ
- Administrador: ([Thiago Trabach](https://github.com/ThiagoTrabach))

## Setup do Projeto
### Etapa 1 - Instalação das dependências 
Na raiz do projeto, execute os comandos abaixo para instalar o dbt:
 1. `poetry shell`
 2. `poetry install`

E o seguinte para instalar os pacotes de dependência do projeto:

 3. `dbt deps`

 ### Etapa 2 - Configuração da autenticação
 3. Obtenha o arquivo de credenciais do Google Cloud `rj-sms-dev-dbt.json`.
 4. Copie o arquivo `./profiles.yml` para o diretório de sua preferência.
 5. Na cópia do arquivo `profiles.yml` altere o path da chave `keyfile` no profile `dev` para onde está armazenada suas credenciais do Google Cloud.
 6. Crie uma variável de ambiente `DBT_PROFILES_DIR` apontando para o diretório onde está a cópia do `profiles.yml` 

    **ex.** DBT_PROFILES_DIR='/Users/foo/.credentials/'


 ### Etapa 3 - Configuração do ambiente de dev
 7. Crie uma variável de ambiente `DBT_USER` com o nome de usuário de sua preferência 
 8. Dê privilegio de execução para o script `./recce.sh`
    - **Linux e MacOS**: `chmod +x recce.sh`
    - **Windows**: Não precisa


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
  - **Linux e MacOS**: Rode `./recce.sh <NOME_BRANCH>`
  - **Windows**: Rode `.\recce.ps1 <NOME_BRANCH>`
- O servidor fica disponível em `localhost:8000`
- Exemplo: no linux eu posso rodar `./recce.sh feat/transforming-vitai-database-into-datalake`
