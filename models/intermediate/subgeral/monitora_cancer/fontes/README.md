# fontes/

Modelos staging-like: 1 por sistema de origem (SISREG, SER, SISCAN) +
parâmetros de procedimentos. Apenas renomeação, tipagem e filtro de
interesse — sem business logic complexa. Alimentam o `UNION` de
`mart_monitora_cancer__fatos`.

| Modelo | Papel |
|---|---|
| `int_monitora_cancer__sisreg` | Eventos da regulação ambulatorial municipal (SISREG). |
| `int_monitora_cancer__ser_ambulatorial` | Eventos da regulação estadual para oncologia (SER). |
| `int_monitora_cancer__siscan` | Laudos de exames de mama (SISCAN). |
| `int_monitora_cancer__siscan_histo_mama` | Laudos histopatológicos de mama (SISCAN). |
| `int_monitora_cancer__parametros_sisreg` | Procedimentos SISREG de interesse + critérios clínicos. |
| `int_monitora_cancer__parametros_ser` | Procedimentos SER de interesse + critérios clínicos. |

> **TODO** (débito pré-existente, fora do escopo da reorganização): os 4
> modelos de sistema-fonte (`sisreg`, `ser_ambulatorial`, `siscan`,
> `siscan_histo_mama`) ainda não têm documentação/testes em
> `_fontes__schema.yml` — só os `parametros_*` estão documentados.
> Documentar em iniciativa separada.
