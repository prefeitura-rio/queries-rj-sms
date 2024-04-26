<!---
Forneça um resumo breve no Título acima. Exemplos de bons títulos de PR:
* "Feature: adicionar modelos tal-e-tal"
* "Correção: deduplicar tal-e-tal"
* "Atualização: versão dbt 0.13.0"
-->

## Descrição & motivação
<!---
Descreva suas alterações e por que você está fazendo elas.
-->

## Issues Relacionadas
<!---
Link para quaisquer issues do GitHub ou tickets relacionados que ajudarão a esclarecer o contexto deste PR e adicionar mais contexto ao seu trabalho.
-->

## Tipo de Mudança
<!-- 
Classifique o tipo de mudança com a qual você está trabalhando para ajudar o revisor a saber no que eles devem ficar de olho
-->
- [ ] Novo modelo
- [ ] Correção de bug
- [ ] Refatoração
- [ ] Mudança que quebra compatibilidade
- [ ] Documentação
- [ ] Atualização/Instalação de dependências


## A fazer antes do merge
<!---
Inclua quaisquer observações sobre coisas que precisam acontecer antes que este PR seja mergeado, por exemplo:
-->
Em caso de quebra de compatibilidade
- [ ] Validar em dev o funcionamento correto dos produtos de dados afetados

## DAG de Linhagem:
<!---
Inclua uma captura de tela da seção relevante do DAG atualizado. Você pode acessar
sua versão do DAG executando `dbt docs generate && dbt docs serve`.
-->

## Validação dos modelos:
<!---
Inclua qualquer saída que confirme que os modelos fazem o que é esperado. Isso pode ser
um link para um dashboard em desenvolvimento em sua ferramenta de BI, ou uma consulta que
compara um modelo existente com um novo.

Use capturas de tela de consultas e resultados que demonstrem o impacto de suas alterações.
Considere usar: consultas ad-hoc, resultados de perfil de dados, estrutura de esquema. Diferença de mudanças
dados de produção quando relevante.
-->

## Considerações de Impacto:
<!---
Se houver modelos dependentes impactados como resultado do seu trabalho, inclua validação
que estes modelos foram/não foram impactados e quais considerações são necessárias,
como notificar partes interessadas.
Assim como na validação dos modelos, use capturas de tela e consultas para ilustrar o impacto.
-->

## Mudanças nos modelos existentes:
<!---
Inclua esta seção se você estiver alterando quaisquer modelos existentes. Link quaisquer
solicitações de pull relacionadas em sua ferramenta de BI, ou instruções para unir (por exemplo, se o antigo
modelos devem ser descartados após a união, ou se uma execução de atualização completa é necessária)
-->

## Lista de Verificação:
<!---
Esta lista de verificação é principalmente útil como um lembrete de pequenas coisas que podem ser facilmente esquecidas - é destinada como uma ferramenta útil em vez de obstáculos a serem superados.
Coloque um `x` em todos os itens que se aplicam, faça notas ao lado de qualquer que não tenham sido
abordados e remova quaisquer itens que não sejam relevantes para este PR.
-->
- [ ] Meu pull request representa uma peça lógica de trabalho.
- [ ] Meus commits estão relacionados ao pull request e parecem limpos.
- [ ] Meu SQL segue o [guia de estilo do dbt Labs](https://github.com/dbt-labs/corp/blob/master/dbt_style_guide.md).
- [ ] `dbt build` completa com sucesso e os testes dbt passam (se não, detalhe por quê)
- [ ] Materializei meus modelos apropriadamente.
- [ ] Adicionei testes e documentação apropriados a quaisquer modelos novos.
- [ ] Adicionei policy tags apropriadas a quaiquer modelos novos.
- [ ] Atualizei o arquivo README.
