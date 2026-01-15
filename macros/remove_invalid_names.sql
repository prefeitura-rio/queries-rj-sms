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

    when REGEXP_CONTAINS(
        REGEXP_REPLACE(
            NORMALIZE({{ text }}, NFD), -- Remove acentos, marcas
            r'[^\p{Letter} ]', -- Substitui tudo que não for letra ou espaço
            ''                 -- por nada
        ),
        r'(?i)^\s*T\s*E\s*S\s*T\s*E\s'
    )
        then null

    -- Uma quantidade absurda de "IGNORADO"s escrito errado
    -- Aqui queremos pegar:
    -- * Basicamente todas as escritas incorretas de 'ignorado' encontradas
    -- * pai/mãe ignorad@
    -- * foi ignorado
    -- * "IGN.. M" ou "IGN.. F" (descrevendo sexo no nome desconhecido)
    -- Mas precisamos ter cuidado pra não apagar nomes reais
    -- ex.: Ignacio, Igor, etc
    when REGEXP_CONTAINS(
        REGEXP_REPLACE(
            NORMALIZE({{ text }}, NFD), -- Remove acentos, marcas
            r'[^\p{Letter}]', -- Substitui tudo que não for letra ou espaço
            ''                 -- por nada
        ),
        r'(?i)^(PAI|E|OU|MAE|FOI)*((IN?G(N|M)?)+(X|M|F)*$|(GNO|IGBNO|IGINO|IGN|IGNA|IGNBO|IGNO|IGNOA|IGNOI|IGNOP|IGNRO|IGONA|IGONO|IGORA|IGTNO|IGUNO|INGNO|INO|UGNO)R*A?N?D?(O|A|AO|OA|OS|R|FO|OU)?$)'
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
        'NAO N',
        ----------------------
        -- Não consta
        ----------------------
        'NAO C',
        'NAO CO',
        'CONSTA',
        'N CONST',
        'N CONSTA',
        'NAO CONST',
        'NAO CONSTA',
        'NAO CONSTA NO RG',
        'NAO CONSTA NO DOC',
        'NAO CONSTA NO DOCUMENTO',
        'FALTA',
        'NAO DIZ',
        -- Typos
        'N COSTA',
        'N COSNTA',
        'NAO CONTA',
        'NAO CONSA',
        'NAO COSTA',
        'NAO COSNTA',
        'N C E',
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
        'NADECLA',
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
        'INFO',
        'INFOR',
        'INFORM',
        'INFORMA',
        'INFORMAR',
        'INFORMADO',
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
        'NAOINFO',
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
        'NAO DITO',
        'NAO DEU',
        -- Typos
        'INFORN',
        'INFOPR',
        'M INF',
        'N FOR',
        'N AO INFORMADO',
        'N IFORMADO',
        'N NIFORMADO',
        'N UNF',
        'N INDF',
        'N INFOT',
        'NAI INF',
        'NAO ,INF',
        'NAO ENF',
        'NAO IF',
        'NAO IFN',
        'NAO IFORMADO',
        'NAO IINF',
        'NAO IMF',
        'NAO IMFORMADO',
        'NAO IN F',
        'NAO INFR',
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
        'NAOIMF',
        'NAOINFF',
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
        'N IDENT',
        'N IDENF',
        'NAI ID',
        'NAO IDENFICADO',
        'NAO IND',
        'NAO INDENT',
        'NAO INDENTIFICADO', 'NAO INDENTIFICADA',
        ----------------------
        -- Não possui/tem/há/trouxe
        ----------------------
        'POSSUI',
        'NAOP',
        'N POSSUI',
        'NAO POSSUI', 'NAOPOSSUI',

        'TEM',
        'NTEM', 'N TEM',
        'NAO T',
        'NAOTEM', 'NAO TEM',
        'TEM NAUM',

        'N HA', 'HA',
        'NAOHA', 'NAO HA',

        'NAO TROUXE', 'NAO TROUXE DOC',

        'N FEZ',
        'NAO FEZ',

        'EXISTE',
        'N EXISTE',
        'NAO EXISTE',
        'INEXISTE',
        -- Typos
        'N ATEM',
        'NAA TEM',
        'NAAO TEM',
        'NAO TE',
        'NAO TM',
        'NAI TEM',
        'NOA TEM',
        'ANO TEM',
        'NAO TEN',
        'NAO TEMN',
        'NA', 'N A',
        'NAO JA',
        'NAI HA',
        'NAP TEM',
        'NAT TEM',
        'NAO TWM',
        'INESISTE',
        ----------------------
        -- Não sabe
        ----------------------
        'NSA',
        'SABE',
        'N SEI',
        'N SABE',
        'NAO SEI',
        'NAO SABE',
        'NAO SABER',
        'NAO SABE INF',
        'NAO SABE INFORMAR',
        'NAO SOUBE',
        'NAO SOUBE INFORMAR',
        'NAO LEMBRA',
        'SEI LA',
        -- Typos
        'SOUBE',
        'NAO SBE',
        'NAO SBAE',
        ----------------------
        -- Sem informação
        ----------------------
        'SI', 'S I', 'S IN',
        'SIN',
        'SINF',
        'SINFO',
        'S INF',
        'S INFO',
        'S INFOR',
        'S INFORM',
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
        'INFORMACAO',
        -- Typos
        'DEM INF',
        'INF SEM',
        'S EM',
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
        'SEMIN',
        'SEMINFOR',
        'SEMM',
        'SEM IMF',
        'SEM IND',
        'SRM INF',
        'SM IN',
        'SM INF',
        'SM INFO',
        ----------------------
        -- Sem nome
        ----------------------
        'SNOME',
        'S NOME',
        'SEM N',
        'SEM NOM',
        'SEM NOME',
        'NOME SEM',
        'S ID',
        'S IDENT',
        'SEM ID',
        'SEM IDEN',
        'SEM IDENT',
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
        'SE NOME',
        'SM NOME',
        'SEN NOME',
        'SEM NME',
        'SME NOME',
        'SE IDE',
        'SE IDEN',
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
        'SEM F',
        'SEM FILIACAO',
        'SMAE', 'S MAE', 'SS MAE',
        'SM MAE',
        'SO MAE', 'SO A MAE',
        'SEM MAE', 'TEM MAE',
        'MAE', 'MAE DE',
        'MAE IG',
        'MAENAO',
        'NOME MAE',
        'MAE VIVA',
        'FALTA MAE',

        'SPAI', 'S PAI', 'SS PAI',
        'SM PAI',
        'SO PAI', 'SO O PAI',
        'SEM PAI', 'TEM PAI',
        'PAI', 'PAI DE',
        'PAI IG',
        'PAINAO',
        'NOME PAI',
        'PAI VIVO',
        'FALTA PAI',

        'SEM NADA',

        'ORFA', 'ORFAO',
        -- Typos
        'SEEM MAE', 'SEEM PAI',
        'SEMM MAE', 'SEMM PAI',
        'SEM PAIO', 'SEM PAOI',
        ----------------------
        -- Desconhecido
        ----------------------
        'DES',
        'DESC',
        'DESC C', 'DESC D',
        'C DESC', 'D DESC',
        'DESCO',
        'DESCON',
        'DESCONH',
        'DESCONHE',
        'DESCONHEC',
        'DESCONHECIDO', 'DESCONHECIDA',
        'CONHECIDO', 'CONHECIDA',
        'PAI DESC',

        'NOME', 'NADA', 'NXX',
        'NE', 'NEM', 'NENHUM',
        'NINGUEM',

        'ANONIMO',
        -- Typos
        'DEC',
        'DSEC',
        'DES C',
        'DESCX',
        'DESXC',
        'DESWC',
        'DEWSC',
        'DESCOHECE',
        'DESCONHCE',
        'DESOCNHCE',
        'DESONHECE',
        'NEHUM',
        'NENHM',
        ----------------------
        -- Mudança/ausência
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
        'INDISP',
        'INDISPONIVEL',
        'NAO ESTA',
        -- Typo
        'OUSENTE',
        'AUSENE',
        'AUSENT',
        'AUSETE',
        'AUDENTE',
        'AUSNETE',
        'AUXENTE',
        'AUJSENTE',
        ----------------------
        -- Óbito
        ----------------------
        'INATIVADO', 'INATIVADA',
        'FALECEU',
        'FALECIDO', 'FALECIDA',
        'FALECIDOS',
        'PACIENTEFALECEU', 'PACIENTE FALECEU',
        'OBITO',
        'MORTO',
        'NAO VIVO',
        -- Typos
        'FELECIDO', 'FELECIDA',
        ----------------------
        -- Descrições
        ----------------------
        'NENEM',
        'CADEIRANTE',
        'PROFESSOR', 'PROFESSORA',
        'ATUALIZADO SMS',
        'CASADO', 'CASADA',
        'MASCULINO', 'FEMININO',
        'PACIENTE',
        'PRESENTE',
        ----------------------
        -- Outros
        ----------------------
        'OUTRO', 'OUTROS',
        'OUTRA', 'OUTRAS',
        'MESMO', 'MESMA',

        'INSERIR',
        'PREENCHER',
        'COMPLETAR',
        'CONFERIR',
        'PENDENTE',
        'PROVISORIO',
        'CADASTRAR',
        'CONFIRMAR',
        'DECLARADO', 'DECLARADA',

        'OCULTO', 'OCULTA',
        'OMITIDO', 'OMITIDA',
        'ILEGIVEL',
        'INVALIDO',

        'PROCESSO',
        'REGISTRO',

        'IS NOT',
        'PROPRIO',
        'RELATOU',
        'INDICADO', 'INDICADA',
        'DEFINIDO',
        'NAO PODE',
        'NAO RESP',
        'SEM FUTURO',
        -- Typos
        'OMITISO',
        'OMOTIDO',
        'DECALRADO',
        ----------------------
        'PLANO EMPRESA',
        'PLANO INDIVIDUAL',
        ----------------------
        'ABC', 'ASD', 'AAA BBB'
        ----------------------
    )
        then null

    else {{ process_null(aux_remove_person_description(text)) }}
end
{% endmacro %}
