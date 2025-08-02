# Documentação do Script Recce

## Visão Geral

O script `recce.sh` é uma ferramenta utilitária projetada para otimizar o fluxo de trabalho de desenvolvimento dbt com integração Recce. Ele automatiza o processo de comparação de modelos dbt entre diferentes branches e ambientes, facilitando a identificação de mudanças e seu impacto.

## Pré-requisitos

- Repositório Git com projeto dbt
- dbt CLI instalado e configurado
- Recce instalado (`pip install recce`)
- Acesso aos ambientes dbt `prod` e `staging` (ou outros)

## Uso

### Uso Básico

```bash
# Use a branch atual com configurações padrão
./tools/recce.sh

# Use a branch atual com ambiente de destino específico
./tools/recce.sh --target staging

# Use a branch atual em modo de teste
./tools/recce.sh --dry-run
```

### Uso Avançado

```bash
# Especifique a branch explicitamente
./tools/recce.sh --branch feature-branch

# Use parâmetros abreviados
./tools/recce.sh -b feature-branch -t dev

# Combine múltiplas opções
./tools/recce.sh -b feature-branch -t staging --dry-run

# Compatibilidade com versões anteriores (argumento posicional)
./tools/recce.sh feature-branch
```

## Parâmetros

| Parâmetro | Abreviado | Descrição | Padrão | Obrigatório |
|-----------|-----------|-----------|--------|-------------|
| `--branch` | `-b` | Nome da branch a ser analisada | Branch atual | Não |
| `--target` | `-t` | Ambiente de destino dbt | `dev` | Não |
| `--dry-run` | - | Executa em modo de teste (pula o dbt build) | `false` | Não |

### Detalhes dos Parâmetros

#### `--branch` / `-b`
Especifica qual branch analisar. Se não fornecida, o script usa a branch atual.

```bash
./tools/recce.sh --branch feature-branch
./tools/recce.sh -b hotfix-123
```

#### `--target` / `-t`
Especifica o ambiente de destino dbt para construção e geração de documentação. Valores comuns incluem:
- `dev` (padrão)
- `staging`

```bash
./tools/recce.sh --target staging
./tools/recce.sh -t dev
```

#### `--dry-run`
Executa o script em modo de teste, mostrando o que seria executado sem realmente executar o comando dbt build. Útil para avaliar mudanças na estrutura da tabela.

```bash
./tools/recce.sh --dry-run
./tools/recce.sh -b feature-branch --dry-run
```

## Exemplos

### Exemplo 1: Fluxo de Trabalho Rápido de Desenvolvimento
```bash
# Você está na feature-branch e quer ver as mudanças
./tools/recce.sh
```
Isso irá:
- Usar sua branch atual
- Usar ambiente de destino `dev`
- Construir modelos modificados
- Iniciar servidor Recce

### Exemplo 2: Teste de Ambiente de Staging
```bash
./tools/recce.sh --target staging
```
Isso irá:
- Usar sua branch atual
- Usar ambiente de destino `staging`
- Construir modelos modificados
- Iniciar servidor Recce

### Exemplo 3: Teste de Configuração
```bash
./tools/recce.sh --branch feature-branch --target dev --dry-run
```
Isso irá:
- Mostrar o que seria executado
- Pular o dbt build real
- Exibir os comandos que seriam executados

### Exemplo 4: Teste de Desenvolvimento
```bash
./tools/recce.sh -b release-candidate -t dev
```
Isso irá:
- Usar a branch `release-candidate`
- Usar ambiente de destino `dev`
- Construir modelos modificados
- Iniciar servidor Recce

## Fluxo de Trabalho

Este script executa o seguinte fluxo de trabalho:

1. **Muda para a branch master** e baixa as últimas alterações
2. **Gera artefatos base** do ambiente de produção
3. **Muda para a branch de destino** (branch atual por padrão)
4. **Constrói modelos modificados** usando comparação de estado dbt
5. **Gera documentação** para o ambiente atual
6. **Inicia o servidor Recce** para comparação visual

## Tratamento de Erros

O script inclui várias verificações de erro:

- **Validação de repositório Git**: Garante que você está em um repositório git
- **Detecção de branch atual**: Valida que a branch atual pode ser determinada
- **Validação de parâmetros**: Verifica argumentos desconhecidos
- **Validação de parâmetros obrigatórios**: Garante que o nome da branch está disponível

## Dicas e Melhores Práticas

1. **Use dry-run primeiro**: Avalie as mudanças na estrutura da tabela com `--dry-run` antes de executar o fluxo completo
2. **Escolha alvos apropriados**: Use `dev` para desenvolvimento, `staging` para homologação
3. **Mantenha a master atualizada**: Garanta que sua branch master esteja atualizada para comparações precisas
4. **Monitore a saída**: Preste atenção na saída colorida para informações de status
5. **Use o Recce efetivamente**: Uma vez que o servidor inicie, use a interface web para analisar mudanças

## Solução de Problemas

### Problemas Comuns

1. **"Não está em um repositório git"**
   - Certifique-se de estar em um diretório de repositório git
   - Execute `git init` se necessário

2. **"Não é possível determinar a branch atual"**
   - Verifique se você está em uma branch válida
   - Execute `git status` para verificar

3. **Falhas no dbt build**
   - Verifique sua configuração dbt
   - Confirme se o ambiente de destino existe em `profiles.yml`
   - Revise os logs dbt para erros específicos

4. **Problemas com servidor Recce**
   - Certifique-se de que o Recce está instalado: `pip install recce`
   - Verifique se a porta já está em uso
   - Confirme se os artefatos dbt foram gerados com sucesso

### Obtendo Ajuda

Se você encontrar problemas:

1. Execute com `--dry-run` para ver o que seria executado
2. Verifique a saída do script para mensagens de erro
3. Verifique sua configuração dbt e perfis
4. Certifique-se de que todos os pré-requisitos estão atendidos

## Localização do Arquivo

O script está localizado em: `tools/recce.sh`

Certifique-se de que o arquivo tem permissões de execução:
```bash
chmod +x tools/recce.sh
``` 