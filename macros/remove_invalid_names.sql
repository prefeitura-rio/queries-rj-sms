{% macro remove_invalid_names(text) %}
case
    -- Nulos comuns ("None", "NULL", etc)
    when {{ process_null(text) }} is null
        then null

    when upper({{text}}) like '%CMS%'
        then null

    -- Valores que são só uma letra, repetida 1 ou mais vezes
    -- ou só uma corrente de dígitos, sem letras (ex. CNS)
    -- Não, não tem como fazer mais bonitinho, o RegEx daqui não suporta backreference
    -- [Ref] https://github.com/google/re2/issues/512
    when REGEXP_CONTAINS(
        REGEXP_REPLACE(
            NORMALIZE({{ text }}, NFD), -- Remove acentos, marcas
            r'[^\p{Letter}0-9]', -- Substitui tudo que não for letra ou dígito
            ''                   -- por nada
        ),
        r'(?i)^(A+|B+|C+|D+|E+|F+|G+|H+|I+|J+|K+|L+|M+|N+|O+|P+|Q+|R+|S+|T+|U+|V+|W+|X+|Y+|Z+|[0-9]+)$'
    )
        then null


    -- Descrições de falta de dados
    when REGEXP_REPLACE(
        TRIM(
            UPPER(
                REGEXP_REPLACE(
                    NORMALIZE({{ text }}, NFD), -- Remove acentos, marcas
                    r'[^\p{Letter} ]', -- Substitui tudo que não for letra ou espaço
                    ''                 -- por nada
                )
            )
        ),
        r'\s{2,}', -- Substitui múltiplos whitespaces
        ' '        -- por um espaço só
    ) in (
        ----------------------
        -- Todos os casos abaixo são exemplos reais com
        -- pelo menos 10 ocorrências na base da Vitai :x
        ----------------------
        -- "Avellar que coisa horrível por que não usar um RegEx ou um LLM ou-"
        -- Porque tem muita variação e complexidade, e eu tenho medo de apagar
        -- nomes de pessoas reais sem querer!!! Fora que um RegEx ficaria ilegível
        ----------------------
        '',
        'SIM', 'NAO',
        'TRUE', 'FALSE',
        ----------------------
        'NC',
        'N C',
        'N CONSTA',
        'NAO CONSTA',
        'NAO CONSTA NO RG',
        'NAO CONSTA NO DOC',
        'NAO CONSTA NO DOCUMENTO',
        -- Typos
        'NAO CONTA',
        'NAO COSTA',
        'NAO COSNTA',
        ----------------------
        'ND', 'NDC',
        'N D',
        'N DECLARADO', 'N DECLARADA',
        'NAO DEC',
        'NAO DECLARO', 'NAO DECLARA',
        'NAO DECLARAD',
        'NAO DECLARADO', 'NAO DECLARADA',
        'NAO DECLARADO NO DOCUMENTO',
        'NAO DECLAROU',
        'NAO DECLARANTE',
        -- Typos
        'N AO DECLARADO',
        'NAO DE CLARADO',
        'NAO DECALARADO',
        'NAO DECALRADO',
        'NAO DECLADO',
        'NAO DECLARACAO',
        'NAO DECLARARDO',
        'NAO DECLARDA',
        'NAO DECLARDO',
        'NAO DECLRADO',
        'NAO DELARADO',
        'NAO DELCARADO',
        ----------------------
        'NI', 'IN', 'NIF', 'N F',
        'N I',
        'N IN',
        'N INF',
        'N INFO',
        'N INFOR',
        'N INFORM',
        'N INFORMA',
        'N INFORMOU',
        'N INFORMADO', 'N INFORMADA',
        'NAO I',
        'NAO IN',
        'NAO INF',
        'NAO INFO',
        'NAO INFOR',
        'NAO INFORM',
        'NAO INFORMA',
        'NAO INFORMAD',
        'NAO INFORMADO', 'NAO INFORMADA',
        'NAO INFORMOU', 'NAO  INFORMOU',
        'NAO INFPELO BOMB', 'NAO INF PELO BOMB',
        'NAO INFORMADO PELO BOMB', 'NAO INFORMADO PELO BOMBEIRO', 
        'NAO INFORMADO PELO ACOMPANHANTE', 'NAO INFORMADO PELA ACOMPANHANTE',
        'NAO INFORMADO SEM DOC',
        'NAO QUIS INFORMAR',
        'NAO FOI INFORMADO',
        -- Typos
        'N AO INFORMADO',
        'N IFORMADO',
        'N NIFORMADO',
        'NAO ,INF',
        'NAO IF',
        'NAO IFN',
        'NAO IFORMADO',
        'NAO IINF',
        'NAO IMF',
        'NAO IMFORMADO',
        'NAO IN F',
        'NAO INDORMADO',
        'NAO INFIRMADO',
        'NAO INFOEMADO',
        'NAO INFOIRMADO',
        'NAO INFOMADO',
        'NAO INFORAMADO',
        'NAO INFORAMDO',
        'NAO INFORMA DO',
        'NAO INFORMACAO',
        'NAO INFORMAFO',
        'NAO INFORMANDO',
        'NAO INFORMAO',
        'NAO INFORMAOD',
        'NAO INFORMAR',
        'NAO INFORMARDO',
        'NAO INFORMDO',
        'NAO INFORME',
        'NAO INFORNADO',
        'NAO INFRMADO',
        'NAO INFROMADO',
        'NAO INOFRMADO',
        'NAO INORMADO',
        'NAO IONFORMADO',
        'NAO NFORMADO',
        'NAO NINFORMADO',
        'NAOINF',
        'NAOINFORMA',
        'NAOINFORMADO',
        'NAOMINF',
        'NSO INF',
        'NSO INFORMADO',
        ----------------------
        'NAO IDEN',
        'NAO IDENT',
        'NAO IDENTI',
        'NAO IDENTIFICADO', 'NAO IDENTIFICADA',
        -- Typos
        'NAO IDENFICADO',
        'NAO IND',
        'NAO INDENT',
        'NAO INDENTIFICADO', 'NAO INDENTIFICADA',
        ----------------------
        'NP',
        'NAO HA',
        'N TEM', 'NAO TEM',
        'NAO POSSUI',
        'NAO TROUXE', 'NAO TROUXE DOC',
        -- Typos
        'NAOPOSSUI',
        ----------------------
        'NS',
        'N S',
        'N SABE',
        'NAO SEI',
        'NAO SABE',
        'NAO SABER',
        'NAO SABE INF',
        'NAO SABE INFORMAR',
        'NAO SOUBE',
        'NAO SOUBE INFORMAR',
        'NAO LEMBRA',
        ----------------------
        'SI',
        'SEM',
        'SEM IN',
        'SEM INF',
        'SEM INFO',
        'SEM INFOR',
        'SEM INFORM',
        'SEM INFORMA',
        'SEM INFORMACA',
        'SEM INFORMACAO',
        'SEM INFORMACOES',
        -- Typos
        'SEM IFORMACAO',
        'SEM INFOMACAO',
        'SEM INFORMADO',
        'SEM INFORMAO',
        'SEM INFORMAAO',
        'SEM INFORMAAAO',
        'SEM INFORMCAO',
        'SEM IMFORMACAO',
        ----------------------
        'SN',
        'SEM N',
        'SEM NOME',
        'SEM IDENTIFICACAO',
        'SEM REGISTRO',
        'SEM CADASTRO',
        'SEM DADOS',
        'SEM DOC',
        'SEM DOCUMENTO',
        'SEM DOCUMENTACAO',
        -- Typos
        'SEM IDENTIFICAO',
        'SEM INDENTIFICACAO',
        ----------------------
        'SEM FILIACAO',
        'SEM MAE', 'SEM PAI',
        ----------------------
        'DESCONHECIDO', 'DESCONHECIDA',
        'IGNORADO', 'IGNORADA',
        ----------------------
        'TESTE', 'TESTE TESTE', 'TESTE TESTE TESTE',
        'TESTE MAE',
        'TESTE NOME SOCIAL',
        ----------------------
        'NR',
        'MUDOU',
        'MUDOU SE', 'MUDOUSE', 'SE MUDOU',
        'MUDOU NITEROI',
        'NAO RESIDE', 'NAO MORA',
        'VIVE COM C',
        'FORA DO TERRITORIO',
        'FORA DE AREA',
        ----------------------
        'PLANO EMPRESA',
        'PLANO INDIVIDUAL',
        ----------------------
        'INATIVADO', 'INATIVADA',
        'FALECEU',
        'FALECIDO', 'FALECIDA',
        'PACIENTEFALECEU', 'PACIENTE FALECEU',
        'OBITO',
        'SEM FUTURO', 'PROVISORIO',
        ----------------------
        'CADEIRANTE',
        'NENEM',
        'PROFESSOR', 'PROFESSORA',
        'ATUALIZADO SMS'
        ----------------------
    )
        then null

    else {{ process_null(aux_remove_person_description(text)) }}
end
{% endmacro %}
