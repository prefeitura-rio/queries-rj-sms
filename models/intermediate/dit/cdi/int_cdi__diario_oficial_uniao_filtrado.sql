{{
    config(
        alias="diario_uniao_filtrado",
        materialized="table",
    )
}}

-- Monta tabela base para envio de emails CDI

-- Filtro preliminar, exclui textos de alguns Ministérios independentemente de conteúdo
with filtros_diario_uniao_prelim as (
    select
        *
    from {{ref('raw_diario_oficial__diarios_uniao_api')}}
    where not (
        starts_with(organizacao_principal, "Ministério da Cultura")
        or starts_with(organizacao_principal, "Ministério do Esporte")
    )
),
-- Diaríos cujo texto não inicia com 'processo'
diario_uniao_geral as (
    select
        * except (texto),
        texto
    from filtros_diario_uniao_prelim
    where not starts_with(upper(texto), "PROCESSO")
),
-- Diários cujo texto inicia com 'processo'
-- Quebra cada processo em '\n', vira entrada separada na tabela
diario_uniao_processos as (
    select
        dou.* except (texto),
        linhas as texto
    from filtros_diario_uniao_prelim as dou,
        unnest(split(dou.texto, '\n')) as linhas
    where starts_with(upper(dou.texto), "PROCESSO")
),

-- Junta ambos novamente para aplicar os filtros finais
diario_uniao_completo as (
    select * from diario_uniao_geral
    union all
    select * from diario_uniao_processos
),
filtros_diario_uniao_final as (
    select
        *
    from diario_uniao_completo
    where (
        -- "Município do Rio de Janeiro"
        -- Tivemos também um caso de "MUNICÍPIO/UF: Rio de Janeiro"
        REGEXP_CONTAINS(
            {{ remove_accents_upper("texto") }},
            r'\bMUNICIPIO[\s\-:]*\/?[\s\-:]*(UF)?[\s\-:]*\b(DO)?\s*RIO\s+DE\s+JANEIRO\b'
        )
        or REGEXP_CONTAINS(
            {{ remove_accents_upper("organizacao_principal") }},
            r'\bMUNICIPIO[\s\-:]*\/?[\s\-:]*(UF)?[\s\-:]*\b(DO)?\s*RIO\s+DE\s+JANEIRO\b'
        )
    )
    and (
        -- Hospital municipal
        REGEXP_CONTAINS(upper(texto), r'\bHOSPITA(L|IS)\s+MUNICIPA(L|IS)\b')
        -- CER, UPA, CF, CMS, Policlínica
        or REGEXP_CONTAINS({{ remove_accents_upper("texto") }}, r'\b(CER|UPA|CF|CMS|POLICLINICA)\b')
        -- Clínica da família
        or REGEXP_CONTAINS({{ remove_accents_upper("texto") }}, r'\bCLINICAS?\s+(DE|DA)\s+FAMILIA\b')
        -- Hospitais federais Cardoso Fontes e de Andaraí
        or REGEXP_CONTAINS({{ remove_accents_upper("texto") }}, r'\bHOSPITAL\s+FEDERAL\s+(CARDO(S|Z)O\s+FONTES|(DE|DO)?\s*ANDARAI)\b')
        -- Daniel Soranz
        or REGEXP_CONTAINS(upper(texto), r'\bDANIEL\s+(RICARDO)?\s*SORANZ\b')
        -- Rodrigo Prado
        or REGEXP_CONTAINS(upper(texto), r'\bRODRIGO\s+(SOU(S|Z)A)?\s*PRADO\b')
        -- Menciona 'saúde'
        or lower(texto)  like "%saúde%"
    )
),

titulos_diario_uniao as (
    select distinct id_oficio, texto_titulo
    from {{ref('raw_diario_oficial__diarios_uniao_api')}}
    where texto_titulo is not null
)

select
    distinct * except(texto_titulo),
    upper(titulos_diario_uniao.texto_titulo) as content_email
from filtros_diario_uniao_final
    left join titulos_diario_uniao
    using (id_oficio)
