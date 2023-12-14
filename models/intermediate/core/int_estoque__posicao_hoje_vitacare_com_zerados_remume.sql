-- posicao do dia de hoje adicionado os materiais remume que estão zerados

with
-- source
    posicao_atual as (select * from {{ ref('raw_prontuario_vitacare__estoque_posicao') }} where
    data_particao = current_date())


-- remume 



select * from posicao_atual