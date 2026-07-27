{% macro remove_invalid_names(text) %}
case
    -- Nulos comuns ("None", "NULL", etc)
    when {{ process_null(text) }} is null
        then null

    -- Vááááários casos de nome de unidade/referência de onde está
    -- ex.: "CLINICA MEDICA CMS EA", "OBSERVACAO CLINICA CMSRPS", "SMS CMS MILTON FONTES MAGARAO", ...
    -- Ao que parece indicando pacientes que não devem ter atendimentos agendados
    -- por serem de outra unidade
    when regexp_contains(
        {{ text }},
        r'(?i)^(SMS|GERENCIA|SETORES|FARMACIA|OC|(SALA\s*DE\s*)?OBSERVACAO(\s*CLINICA)?|CLINICA\s*MEDICA|(SALA\s*DE\s*)?CURATIVOS?|SAUDE\s*BUCAL|ODONTO(LOGIA)?|PROCEDIMENTOS?|EQUIPE(\s+[A-Z]+)?|(PACIENTES?)?\s*(D[AOE]|PERTENCE(\s*AO)?|FORA(\s*D[AOE])?(\s*AREA)?)?|MUDOU|POLO\s*DE\s*[A-Z]*|[\-\s])*(CMS|CF)'
    )
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
        r'(?i)^\s*T\s*E\s*S\s*T\s*E\b'
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
            r'[^\p{Letter}]', -- Substitui tudo que não for letra
            ''                 -- por nada
        ),
        r'(?i)^(PAI|E|OU|MAE|FOI)*((IN?G(N|M)?)+(X|M|F)*$|(GNO|IGBNO|IGINO|IGN|IGNA|IGNBO|IGNO|IGNOA|IGNOI|IGNOP|IGNRO|IGONA|IGONO|IGORA|IGTNO|IGUNO|INGNO|INO|UGNO)R*A?N?D?(O|A|AO|OA|OS|R|FO|OU)?$)'
    )
        then null

    -- Descrições de falta de dados
    when 
    UPPER(
        REGEXP_REPLACE(
            NORMALIZE({{ text }}, NFD), -- Remove acentos, marcas
            r'[^\p{Letter}]', -- Substitui tudo que não for letra
            ''                -- por nada
        )
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
        'NAP', 'NAAO', 'NAON',
        ----------------------
        -- Não consta
        ----------------------
        'NAOC',
        'NAOCO',
        'CONSTA',
        'NCONST',
        'NCONSTA',
        'NAOCONST',
        'NAOCONSTA',
        'NAOCONSTANORG',
        'NAOCONSTANODOC',
        'NAOCONSTANODOCUMENTO',
        'FALTA',
        'NAODIZ',
        -- Typos
        'NCOSTA',
        'NCOSNTA',
        'NAOCONTA',
        'NAOCONSA',
        'NAOCOSTA',
        'NAOCOSNTA',
        'NCE',
        ----------------------
        'NDO', 'NDA',
        'NDEC',
        'NDECL',
        'NDECLARADO', 'NDECLARADA',
        'NAOD',
        'NAODE',
        'NAODEC',
        'NAODECLARO', 'NAODECLARA',
        'NAODECLARAD',
        'NAODECLARADO', 'NAODECLARADA',
        'NAODECLARADONODOCUMENTO',
        'NAODECLAROU',
        'NAODECLARANTE',
        -- Typos
        'NADECLA',
        'NAODECALARADO',
        'NAODECALRADO',
        'NAODECLADO',
        'NAODECLARACAO',
        'NAODECLARARDO',
        'NAODECLARDA',
        'NAODECLARDO',
        'NAODECLRADO',
        'NAODELARADO',
        'NAODELCARADO',
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
        'NIN',
        'NINF',
        'NINFO',
        'NINFOR',
        'NINFORM',
        'NINFORMA',
        'NINFORMOU',
        'NINFORMADO', 'NINFORMADA',
        'NAOI',
        'NAOIN',
        'NAOINF',
        'NAOINFO',
        'NAOINFOR',
        'NAOINFORM',
        'NAOINFORMA',
        'NAOINFORMAD',
        'NAOINFORMADO', 'NAOINFORMADA',
        'NAOINFORMOU',
        'NAOINFPELOBOMB', 'NAOINFPELOBOMB',
        'NAOINFORMADOPELOBOMB',
        'NAOINFORMADOPELOBOMBEIRO',
        'NAOINFORMADOPELOACOMPANHANTE', 'NAOINFORMADOPELAACOMPANHANTE',
        'NAOINFORMADOSEMDOC',
        'NAOQUISINFORMAR',
        'NAOFOIINFORMADO',
        'NAODISSE',
        'NAODITO',
        'NAODEU',
        -- Typos
        'INFORN',
        'INFOPR',
        'MINF',
        'NFOR',
        'NIFORMADO',
        'NNIFORMADO',
        'NUNF',
        'NINDF',
        'NINFOT',
        'NAIINF',
        'NAOENF',
        'NAOIF',
        'NAOIFN',
        'NAOIFORMADO',
        'NAOIINF',
        'NAOIMF',
        'NAOIMFORMADO',
        'NAOINFR',
        'NAOINDORMADO',
        'NAOINFIRMADO',
        'NAOINFOEMADO',
        'NAOINFOIRMADO',
        'NAOINFOMADO',
        'NAOINFORAMADO',
        'NAOINFORAMDO',
        'NAOINFORMACAO',
        'NAOINFORMAFO',
        'NAOINFORMANDO',
        'NAOINFORMAO',
        'NAOINFORMAOD',
        'NAOINFORMAR',
        'NAOINFORMARDO',
        'NAOINFORMDO',
        'NAOINFORME',
        'NAOINFORNADO',
        'NAOINFRMADO',
        'NAOINFROMADO',
        'NAOINOFRMADO',
        'NAOINORMADO',
        'NAOIONFORMADO',
        'NAONFORMADO',
        'NAONINFORMADO',
        'NAOIMF',
        'NAOINFF',
        'NAONF',
        'NAOMINF',
        'NSOINF',
        'NSOINFORMADO',
        'NOAINF',
        ----------------------
        -- Não identificado
        ----------------------
        'NID',
        'NAOID',
        'NAOIDE',
        'NAOIDEN',
        'NAOIDENT',
        'NAOIDENTI',
        'NAOIDENTIFICADO', 'NAOIDENTIFICADA',
        -- Typos
        'NIDENT',
        'NIDENF',
        'NAIID',
        'NAOIDENFICADO',
        'NAOIND',
        'NAOINDENT',
        'NAOINDENTIFICADO', 'NAOINDENTIFICADA',
        ----------------------
        -- Não possui/tem/há/trouxe
        ----------------------
        'POSSUI',
        'NAOP',
        'NPOSSUI',
        'NAOPOSSUI',

        'TEM',
        'NTEM',
        'NAOT',
        'NAOTEM',
        'TEMNAUM',

        'NHA', 'HA',
        'NAOHA',

        'NAOTROUXE', 'NAOTROUXEDOC',

        'NFEZ',
        'NAOFEZ',

        'EXISTE',
        'NEXISTE',
        'NAOEXISTE',
        'INEXISTE',
        -- Typos
        'NATEM',
        'NAATEM',
        'NAAOTEM',
        'NAOTE',
        'NAOTM',
        'NAITEM',
        'NOATEM',
        'ANOTEM',
        'NAOTEN',
        'NAOTEMN',
        'NA',
        'NAOJA',
        'NAIHA',
        'NAPTEM',
        'NATTEM',
        'NAOTWM',
        'INESISTE',
        ----------------------
        -- Não sabe
        ----------------------
        'NSA',
        'SABE',
        'NSEI',
        'NSABE',
        'NAOSEI',
        'NAOSABE',
        'NAOSABER',
        'NAOSABEINF',
        'NAOSABEINFORMAR',
        'NAOSOUBE',
        'NAOSOUBEINFORMAR',
        'NAOLEMBRA',
        'SEILA',
        -- Typos
        'SOUBE',
        'NAOSBE',
        'NAOSBAE',
        ----------------------
        -- Sem informação
        ----------------------
        'SI',
        'SIN',
        'SINF',
        'SINFO',
        'SINF',
        'SINFO',
        'SINFOR',
        'SINFORM',
        'SEM',
        'SEMI',
        'SEMIN',
        'SEMINF',
        'SEMINFO',
        'SEMINFOR',
        'SEMINFORM',
        'SEMINFORMA',
        'SEMINFORMACA',
        'SEMINFORMACAO',
        'SEMINFORMACOES',
        'INFORMACAO',
        -- Typos
        'DEMINF',
        'INFSEM',
        'SE',
        'SEINF',
        'SEN',
        'SENINF',
        'SEMNF',
        'SEMIF',
        'SEMIR',
        'SEMIFORMACAO',
        'SEMINFOMACAO',
        'SEMINFORMADO',
        'SEMINFORMAO',
        'SEMINFORMAAO',
        'SEMINFORMAAAO',
        'SEMINFORMCAO',
        'SEMIM',
        'SEMIMFORMACAO',
        'SEMIF',
        'SEMIN',
        'SEMINFOR',
        'SEMM',
        'SEMIMF',
        'SEMIND',
        'SRMINF',
        'SMIN',
        'SMINF',
        'SMINFO',
        ----------------------
        -- Sem nome
        ----------------------
        'SNOME',
        'SEMN',
        'SEMNOM',
        'SEMNOME',
        'NOMESEM',
        'SID',
        'SIDENT',
        'SEMID',
        'SEMIDEN',
        'SEMIDENT',
        'SEMIDENTIFICACAO',
        'SEMR',
        'SEMRG',
        'SEMREG',
        'SEMREGISTRO',
        'SEMCADASTRO',
        'SEMDOC',
        'SEMDOCUMENTO',
        'SEMDOCUMENTACAO',
        'SEMDADOS',
        -- Typos
        'SENOME',
        'SMNOME',
        'SENNOME',
        'SEMNME',
        'SMENOME',
        'SEIDE',
        'SEIDEN',
        'SENRG',
        'SEMR',
        'SEMIDENTIFICAO',
        'SEMINDENTIFICACAO',
        ----------------------
        -- Não registrado
        ----------------------
        'NREG',
        'NAOREG',
        ----------------------
        -- Sem filiação/pai/mãe
        ----------------------
        'SEMF',
        'SEMFILIACAO',
        'SMAE', 'SSMAE',
        'SMMAE',
        'SOMAE', 'SOAMAE',
        'SEMMAE', 'TEMMAE',
        'MAE', 'MAEDE',
        'MAEIG',
        'MAENAO',
        'NOMEMAE',
        'MAEVIVA',
        'FALTAMAE',

        'SPAI', 'SSPAI',
        'SMPAI',
        'SOPAI', 'SOOPAI',
        'SEMPAI', 'TEMPAI',
        'PAI', 'PAIDE',
        'PAIIG',
        'PAINAO',
        'NOMEPAI',
        'PAIVIVO',
        'FALTAPAI',

        'SEMNADA',

        'ORFA', 'ORFAO',
        -- Typos
        'SEEMMAE', 'SEEMPAI',
        'SEMMMAE', 'SEMMPAI',
        'SEMPAIO', 'SEMPAOI',
        ----------------------
        -- Desconhecido
        ----------------------
        'DES',
        'DESC',
        'DESCC', 'DESCD',
        'CDESC', 'DDESC',
        'DESCO',
        'DESCON',
        'DESCONH',
        'DESCONHE',
        'DESCONHEC',
        'DESCONHECIDO', 'DESCONHECIDA',
        'CONHECIDO', 'CONHECIDA',
        'PAIDESC',

        'NOME', 'NADA', 'NXX',
        'NE', 'NEM', 'NENHUM',
        'NINGUEM',

        'ANONIMO',
        -- Typos
        'DEC',
        'DSEC',
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
        'NAOMORA',
        'MUDOU',
        'MUDOUSE', 'MUDOUSE', 'SEMUDOU',
        'MUDOUNITEROI',
        'FOI',
        'NAORESIDE',
        'VIVECOMC',
        'FORADOTERRITORIO',
        'FORADEAREA',
        'AUSENTE',
        'INDISP',
        'INDISPONIVEL',
        'NAOESTA',
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
        'PACIENTEFALECEU',
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

        'ISNOT',
        'PROPRIO',
        'RELATOU',
        'INDICADO', 'INDICADA',
        'DEFINIDO',
        'NAOPODE',
        'NAORESP',
        'SEMFUTURO',
        -- Typos
        'OMITISO',
        'OMOTIDO',
        'DECALRADO',
        ----------------------
        'PLANOEMPRESA',
        'PLANOINDIVIDUAL',
        ----------------------
        'ABC', 'ASD', 'AAABBB'
        ----------------------
    )
        then null

    else trim(regexp_replace(
        {{ process_null(aux_remove_person_description(text)) }},
        r"(?i)\bATENDID[AO]\s*PELO\s*CMS\b",
        ""
    ))
    -- TODO: filtrar nomes de CFs/CMSs do final
end
{% endmacro %}
