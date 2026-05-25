# 📊 Queries SMS/RJ
> Ambiente para modelagem de dados da **SMS Rio** com **dbt**, **BigQuery** e comparação de ambientes via **Recce**.

Administrador: **[Pedro Marques](https://github.com/TanookiVerde)**  

---

## 🛠️ Pré-requisitos

| Ferramenta | Versão | Observações |
|------------|--------|-------------|
| **Python** | 3.10.x | Windows: Baixe o instalador https://www.python.org/downloads/release/python-3109/ |
| **Poetry** | 1.7.1  | `pip install poetry==1.7.1` |
| **dbt-core** + **dbt-bigquery** |  | `pip install dbt-core dbt-bigquery` |
| **Recce** | | Ferramenta para checar a diferença entre os dados em diversos ambientes <br> |
| **Git** | | Windows: Baixe o instalador https://git-scm.com/downloads/win |

> **Clone o repositório**
> ```bash
> git clone https://github.com/prefeitura-rio/queries-rj-sms
> cd queries-sms-rj
> ```  

---

## ⚙️ Instalação passo-a-passo

### 1 - Criar o ambiente Poetry
```bash
poetry shell              # cria/ativa o venv isolado
poetry install --no-root  # instala todas as dependências declaradas em pyproject.toml
```

O comando `poetry shell` garante que as libs sejam instaladas no ambiente virtual correto, evitando conflitos.


### 2 - Instalar pacotes dbt
```bash
dbt deps              # baixa dbt-utils e demais packages declarados em packages.yml
```

`dbt deps` resolve versões e coloca tudo em .dbt_packages/.

### 3 - Configurar credenciais do Google Cloud
1. Obtenha o arquivo `rj-sms-dev-dbt.json` (IAM → Service Accounts).

2. Copie o arquivo `profiles.yml` para um diretório seguro de sua preferência.

3. Edite o parâmetro keyfile no profile `dev` do arquivo `profiles.yml` para apontar para o JSON.
    - No Windows, coloque o path completo entre aspas duplas.

5. Crie uma variável de ambiente chamada `DBT_PROFILES_DIR` que aponte para o caminho do arquivo `profiles.yml`
    - **ex.** `DBT_PROFILES_DIR='/Users/foo/.credentials/'` 


### 4 - Configure seu ambiente de desenvolvimento

1. Crie uma variável de ambiente chamada `DBT_USER`, que receba seu nome.
    - **ex.** `DBT_USER='seu_nome'`

2. **[OBRIGATÓRIO]** Crie uma variável de ambiente chamada `HASH_SECRET` com um valor secreto aleatório.
    - Esta variável é **obrigatória** para anonimização de dados sensíveis (CPF, IDs, etc.)
    - Use um valor aleatório e seguro (mínimo 32 caracteres)
    - **NUNCA commite este valor no Git!**
    
    **Gerar um secret seguro:**
    ```bash
    # Linux/macOS
    openssl rand -hex 32
    
    # Python
    python -c "import secrets; print(secrets.token_hex(32))"
    ```
    
    **Configurar a variável:**
    ```bash
    # Linux/macOS (adicione ao ~/.zshrc ou ~/.bashrc para permanência)
    export HASH_SECRET='seu_secret_gerado_aqui'
    
    # Windows PowerShell
    $env:HASH_SECRET='seu_secret_gerado_aqui'
    
    # Windows CMD
    set HASH_SECRET=seu_secret_gerado_aqui
    ```
    
    **Ou use um arquivo `.env` na raiz do projeto** (recomendado):
    ```bash
    # .env
    HASH_SECRET=seu_secret_gerado_aqui
    DBT_USER=seu_nome
    ```
    
    Depois carregue antes de executar o dbt:
    ```bash
    # Linux/macOS
    export $(cat .env | xargs) && dbt run
    ```

3. Dê privilégio de execução para o script ./recce.sh
    - **Linux e MacOS**: `chmod +x tools/recce.sh`
    - **Windows**: Não precisa

---

## 💡 Dicas
1. Use a extensão ([Power User for dbt](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user)) no vscode ou similares, para ter acesso a uma interface gráfica para interagir com o dbt.

2. Cheque se seu ambiente está executando com o compilador certo (Python 3.10.x)

---

## 🏗️ Fluxo de trabalho com o dbt
| Ação                 | Comando básico                        | Exemplos úteis             |
| -------------------- | ------------------------------------- | -------------------------- |
| **Executar modelos** | `dbt run`                             | `dbt run -s "nome_modelo"` |
| **Rodar testes**     | `dbt test`                            | `dbt test -s tag:sua_tag`  |
| **Executar e testar**| `dbt build`                           | `dbt build -s staging.*`   |
| **Gerar docs HTML**  | `dbt docs generate && dbt docs serve` | Abre em `localhost:8080`   |

---

## 🔍 Fluxo de trabalho com o recce
A ferramenta spawna um contêiner e publica a interface em `http://localhost:8000`.
Assim você avalia o diff entre produção e a sua branch antes mesmo do merge, sendo útil para avaliar a extensão das suas alterações no fluxo de transformação de dados.

[Passo a passo de como utilizar o recce](tools/recce.md)

---

## 🤝 Como contribuir com o projeto
Não esqueça de checar se você está logado no seu ambiente com a sua conta certa do GitHub.  
(A que você quer usar para trabalhar nesse projeto).
### 1 - Fork & Branch
- Crie branches no formato `feat/<breve-descrição>` ou `fix/<issue>`
### 2 - Commits semânticos
- `feat:<descricao>`, `fix:<descricao>`, `refact:<descricao>`, ...
### 3 - Execute build e rode o recce para checar que está tudo certo e não quebrou nada

### 4 - Abra o Pull Request
 - Descreva brevemente o contexto e a solução
 - Adicione screenshot do Recce se aplicável

---

## Qualquer dúvida, erro, crítica ou sugestão:
### Basta entrar em contato com o [Administrador](https://github.com/TanookiVerde) ❤️
