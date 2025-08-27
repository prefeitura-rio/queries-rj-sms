{% macro is_same_name(a, b) %}
  (
    -- Remove acentos, marcas, e tudo que não for letra, e compara case insensitive
    -- Bom pra mitigar um pouco as mil variações em nomes sociais de mentirinha
    -- ex.: "João Sant'Anna" = "joao sant anna"
    (
      UPPER(REGEXP_REPLACE(NORMALIZE({{ a }}, NFD), r'[^\p{Letter}]', ''))
      =
      UPPER(REGEXP_REPLACE(NORMALIZE({{ b }}, NFD), r'[^\p{Letter}]', ''))
    )
    -- Muitos casos de typos de uma única letra
    -- ex.: Mateus -> Matheus; Sousa -> Souza; Ana -> Anna
    -- Temos que tomar cuidado aqui, porque podemos acabar eliminando nomes reais
    -- ex.: EDIT_DISTANCE('Vitor', 'Vitoria') = 2
    or (
      EDIT_DISTANCE(UPPER({{ a }}), UPPER({{ b }})) = 1
    )
  )
{% endmacro %}
