{% macro validate_cns(cns) %}
-- [Ref] https://www.gov.br/ans/pt-br/centrais-de-conteudo/manuais-do-portal-operadoras/sib-manual-de-instalacao-historico-de-versao-e-outros-arquivos/manual/algoritmos-do-aplicativo-de-carga
case
  when {{ cns }} is null
    then false

  when length({{ cns }}) != 15
    then false

  -- Um único dígito repetido 15 vezes
  when regexp_contains({{ cns }}, r"^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$")
    then false

  -- Rejeita strings com qualquer coisa além de dígitos
  -- (senão teríamos que tratar em cada uso abaixo)
  when regexp_replace({{ cns }}, r"[^0-9]", "") != {{ cns }}
    then false

  -- CNS provisório
  when mod(
    (
      cast(substr({{ cns }}, 1, 1) as INT64) * 15
      + cast(substr({{ cns }}, 2, 1) as INT64) * 14
      + cast(substr({{ cns }}, 3, 1) as INT64) * 13
      + cast(substr({{ cns }}, 4, 1) as INT64) * 12
      + cast(substr({{ cns }}, 5, 1) as INT64) * 11
      + cast(substr({{ cns }}, 6, 1) as INT64) * 10
      + cast(substr({{ cns }}, 7, 1) as INT64) * 9
      + cast(substr({{ cns }}, 8, 1) as INT64) * 8
      + cast(substr({{ cns }}, 9, 1) as INT64) * 7
      + cast(substr({{ cns }}, 10, 1) as INT64) * 6
      + cast(substr({{ cns }}, 11, 1) as INT64) * 5
      + cast(substr({{ cns }}, 12, 1) as INT64) * 4
      + cast(substr({{ cns }}, 13, 1) as INT64) * 3
      + cast(substr({{ cns }}, 14, 1) as INT64) * 2
      + cast(substr({{ cns }}, 15, 1) as INT64)
    ),
    11
  ) = 0
    then true

  -- CNS permanente
  else (
    -- (3) Retorna comparação do CNS recebido com o CNS "corrigido"
    select
      case
        -- if digito_verificador != 10:
        --   return cns == f"{pis}000{digito_verificador}"
        when digito_verificador != 10
          then {{ cns }} = concat(pis, "000", digito_verificador)
        -- else:
        --   return cns == f"{pis}001{ 11 - ((soma + 2) % 11) }"
        else (
          {{ cns }} = concat(pis, "001", 11 - mod(soma + 2, 11))
        )
      end
    from (
      -- (2) Calcula dígito verificador, que depende da soma
      select
        pis,
        soma,
        case
          when mod(soma, 11) = 0
            then 0
          else 11 - mod(soma, 11)
        end as digito_verificador
      from (
        -- (1) Obtém PIS, calcula somatório tal qual no CNS provisório,
        --     mas somente dos primeiros 11 dígitos
        select
          substr({{ cns }}, 1, 11) as pis,
          -- Aqui precisamos de safe_cast() ao invés de cast() mesmo já
          -- tendo eliminado casos inválidos, porque o BigQuery otimiza
          -- subqueries de uma forma misteriosa, roda sempre, e dá erro
          (
            safe_cast(substr({{ cns }}, 1, 1) as INT64) * 15
            + safe_cast(substr({{ cns }}, 2, 1) as INT64) * 14
            + safe_cast(substr({{ cns }}, 3, 1) as INT64) * 13
            + safe_cast(substr({{ cns }}, 4, 1) as INT64) * 12
            + safe_cast(substr({{ cns }}, 5, 1) as INT64) * 11
            + safe_cast(substr({{ cns }}, 6, 1) as INT64) * 10
            + safe_cast(substr({{ cns }}, 7, 1) as INT64) * 9
            + safe_cast(substr({{ cns }}, 8, 1) as INT64) * 8
            + safe_cast(substr({{ cns }}, 9, 1) as INT64) * 7
            + safe_cast(substr({{ cns }}, 10, 1) as INT64) * 6
            + safe_cast(substr({{ cns }}, 11, 1) as INT64) * 5
          ) as soma
      )
    )
  )
end
{% endmacro %}
