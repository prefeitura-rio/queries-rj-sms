{% macro remove_invalid_names(text) %}
case
    -- Nulos comuns ("None", "NULL", etc)
    when {{ process_null(text) }} is null
        then null

    when upper({{text}}) like '%CMS%'
        then null

    -- Textos que são só uma letra, repetida 1 ou mais vezes
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

    -- Textos que são somente consoantes
    -- Não podemos considerar W e Y como consoantes porque
    -- existem 'Wlly's (https://nomesdobrasil.net/nomes/wlly) etc
    when '' = REGEXP_REPLACE(
        NORMALIZE({{ text }}, NFD), -- Remove acentos, marcas
        r'(?i)[^AEIOUWY]', -- Substitui tudo que não for vogal (com leniência pra W/Y)
        ''                 -- por nada
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
        -- múltiplas ocorrências :x
        ----------------------
        -- "Avellar que coisa horrível por que não usar um RegEx ou um LLM ou-"
        -- Porque tem muita variação e complexidade, e eu tenho medo de apagar
        -- nomes de pessoas reais sem querer!!! Fora que um RegEx ficaria ilegível
        ----------------------
        '',
        'SIM', 'NAO',
        'TRUE', 'FALSE',
        -- Typos
        'NAP', 'NAAO', 'N A O',
        ----------------------
        -- Não consta
        ----------------------
        'CONSTA', 'N CONSTA',
        'NAO CONSTA',
        'NAO CONSTA NO RG',
        'NAO CONSTA NO DOC',
        'NAO CONSTA NO DOCUMENTO',
        'FALTA',
        'NAO DIZ',
        -- Typos
        'NAO CONTA',
        'NAO COSTA',
        'NAO COSNTA',
        ----------------------
        'NDO', 'NDA',
        'N DEC',
        'N DECL',
        'N DECLARADO', 'N DECLARADA',
        'NAO D',
        'NAO DE',
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
        -- Não informa
        ----------------------
        'NI', 'IN',
        'NIF', 'INF',
        'N I',
        'N IN',
        'N INF',
        'N INFO',
        'N INFOR',
        'N INFORM',
        'N INFORMA',
        'N INFORMOU',
        'N INFORMADO', 'N INFORMADA',
        'NINF',
        'NINFO',
        'NAO I',
        'NAO IN',
        'NAO INF',
        'NAO INFO',
        'NAO INFOR',
        'NAO INFORM',
        'NAO INFORMA',
        'NAO INFORMAD',
        'NAO INFORMADO', 'NAO INFORMADA',
        'NAO INFORMOU',
        'NAO INFPELO BOMB', 'NAO INF PELO BOMB',
        'NAO INFORMADO PELO BOMB',
        'NAO INFORMADO PELO BOMBEIRO',
        'NAO INFORMADO PELO ACOMPANHANTE', 'NAO INFORMADO PELA ACOMPANHANTE',
        'NAO INFORMADO SEM DOC',
        'NAO QUIS INFORMAR',
        'NAO FOI INFORMADO',
        'NAO DISSE',
        -- Typos
        'M INF',
        'N FOR',
        'N AO INFORMADO',
        'N IFORMADO',
        'N NIFORMADO',
        'N UNF',
        'NAI INF',
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
        'NAO NF',
        'NAO NFORMADO',
        'NAO NINFORMADO',
        'NAOIN',
        'NAOINF',
        'NAOINFORMA',
        'NAOINFORMADO',
        'NAONF',
        'NAOMINF',
        'NSO INF',
        'NSO INFORMADO',
        'NOA INF',
        ----------------------
        -- Não identificado
        ----------------------
        'N ID',
        'NAO ID',
        'NAO IDE',
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
        -- Não possui/tem/há/trouxe
        ----------------------
        'NAOP',
        'NAO POSSUI', 'NAOPOSSUI',

        'TEM',
        'NTEM', 'N TEM',
        'NAOTEM', 'NAO TEM',

        'N HA', 'HA',
        'NAOHA', 'NAO HA',

        'NAO TROUXE', 'NAO TROUXE DOC',
        -- Typos
        'NAO TE',
        'NAO TM',
        'NAI TEM',
        'NOA TEM',
        'NA', 'N A',
        ----------------------
        -- Não sabe
        ----------------------
        'NSA',
        'SABE',
        'N SABE',
        'NAO SEI',
        'NAO SABE',
        'NAO SABER',
        'NAO SABE INF',
        'NAO SABE INFORMAR',
        'NAO SOUBE',
        'NAO SOUBE INFORMAR',
        'NAO LEMBRA',
        -- Typos
        'SOUBE',
        ----------------------
        -- Sem informação
        ----------------------
        'SI', 'S I', 'S IN', 'SIN',
        'SINF',
        'S INF',
        'S INFO',
        'S INFOR',
        'SEM', 'S E M',
        'SEM I',
        'SEM IN',
        'SEM INF', 'SEMINF',
        'SEM INFO', 'SEMINFO',
        'SEM INFOR',
        'SEM INFORM',
        'SEM INFORMA',
        'SEM INFORMACA',
        'SEM INFORMACAO',
        'SEM INFORMACOES',
        'INFO',
        'INFOR',
        'INFORMA',
        'INFORMACAO',
        -- Typos
        'SE',
        'SE INF',
        'SEN',
        'SEN INF',
        'SEM NF',
        'SEM IF',
        'SEM IR',
        'SEM IFORMACAO',
        'SEM INFOMACAO',
        'SEM INFORMADO',
        'SEM INFORMAO',
        'SEM INFORMAAO',
        'SEM INFORMAAAO',
        'SEM INFORMCAO',
        'SEM IM',
        'SEM IMFORMACAO',
        'SEMIF',
        'SEM IMF',
        'SRM INF',
        'SM IN',
        'SM INF',
        'SM INFO',
        ----------------------
        -- Sem nome
        ----------------------
        'SEMM',
        'S NOME',
        'SEM N',
        'SEM NOME',
        'S ID',
        'SEM ID',
        'SEM IDENTIFICACAO',
        'SEM R',
        'SEM RG',
        'SEM REG',
        'SEM REGISTRO',
        'SEM CADASTRO',
        'SEM DOC',
        'SEM DOCUMENTO',
        'SEM DOCUMENTACAO',
        'SEM DADOS',
        -- Typos
        'SEN RG',
        'SEMR',
        'SEM IDENTIFICAO',
        'SEM INDENTIFICACAO',
        ----------------------
        -- Não registrado
        ----------------------
        'N REG',
        'NAO REG',
        ----------------------
        -- Sem filiação/pai/mãe
        ----------------------
        'SEM FILIACAO',
        'SMAE', 'S MAE',
        'SEM MAE', 'TEM MAE',
        'MAE', 'MAE DE',
        'MAE IG',
        'SPAI', 'S PAI',
        'SEM PAI', 'TEM PAI',
        'PAI', 'PAI DE',
        'PAI IG',
        'ORFA', 'ORFAO',
        ----------------------
        -- Desconhecido
        ----------------------
        'DES',
        'DESC',
        'DESC C', 'DESC D',
        'C DESC', 'D DESC',
        'DESCO',
        'DESCON',
        'DESCONHECIDO', 'DESCONHECIDA',

        'NOME', 'NADA', 'NXX',
        'NE', 'NEM', 'NENHUM',
        'NAO C',

        'IG', 'I G',
        'IGN',
        'IGNO',
        'IGNORA', 'IGNORO',
        'IGNORADO', 'IGNORADA',
        'IGNOROU',
        -- Typos
        'DEC',
        'DSEC',
        'DES C',
        'NEHUM',
        'NENHM',
        'IGM',
        'ING',
        'IGORADO', 'IGORADA',
        'IGNORDO',
        'GNORADO',
        ----------------------
        -- Mudança
        ----------------------
        'NAO MORA',
        'MUDOU',
        'MUDOU SE', 'MUDOUSE', 'SE MUDOU',
        'MUDOU NITEROI',
        'FOI',
        'NAO RESIDE',
        'VIVE COM C',
        'FORA DO TERRITORIO',
        'FORA DE AREA',
        'AUSENTE',
        ----------------------
        -- Óbito
        ----------------------
        'INATIVADO', 'INATIVADA',
        'FALECEU',
        'FALECIDO', 'FALECIDA',
        'PACIENTEFALECEU', 'PACIENTE FALECEU',
        'OBITO',
        'MORTO',
        'SEM FUTURO', 'PROVISORIO',
        ----------------------
        -- Descrições
        ----------------------
        'NENEM',
        'CADEIRANTE',
        'PROFESSOR', 'PROFESSORA',
        'ATUALIZADO SMS',
        'OUTRO', 'OUTROS',
        'OUTRA', 'OUTRAS',
        'MESMO', 'MESMA',
        'CASADO', 'CASADA',
        ----------------------
        'TESTE', 'TESTE TESTE', 'TESTE TESTE TESTE',
        'TESTE MAE',
        'TESTE NOME SOCIAL',
        ----------------------
        'PLANO EMPRESA',
        'PLANO INDIVIDUAL',
        ----------------------
        'ABC', 'ASD'
        ----------------------
    )
        then null

    else {{ process_null(aux_remove_person_description(text)) }}
end
{% endmacro %}
