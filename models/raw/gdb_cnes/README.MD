# GDB CNES

Dados do CNES advindos dos backups mensais, em formato GDB, providos pela CGCCA.

> [!WARNING]
> As tabelas nessa camada possuem dados de TODAS as competências. Cada competência é um valor de `data_particao` diferente.

> [!WARNING]
> Não é suficiente deduplicar somente por `data_particao`! Competências podem receber dados múltiplas vezes, a depender de revisões eventuais. É importante também deduplicar por algum identificador, ordenado por data/hora de carregamento mais recente, `_loaded_at`.
