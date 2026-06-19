{% macro remove_invalid_email(texto) %}
case
    -- Remove nulos comuns: null, none, vazio etc.
    when {{ process_null(texto) }} is null
        then null

    -- Remove e-mails sem arroba ou sem domínio
    when split(trim({{ texto }}), '@')[safe_offset(1)] is null
        then null

    -- Remove e-mails em que a parte antes do @ tem número colado com termo de contato.
    -- Exemplos: 21999999999zapnome@example.com, 21999999999sozap@example.com,
    -- 21999999999whatsnome@example.com, usuario21999999999wpp@example.com.
    when regexp_contains(
        upper(
            regexp_replace(
                normalize(
                    split({{ texto }}, '@')[safe_offset(0)],
                    NFD
                ),
                r'[^\p{L}0-9]',
                ''
            )
        ),
        r'[0-9]+(SO)?(ZAP|ZAPZAP|WHATS|WHATSAPP|WPP)'
    )
        then null

    -- Remove e-mails em que a parte antes do @ indica telefone/celular/contato via zap/whats.
    -- Exemplos: telefonezozap@dominio.gmail, telefonezap@example.com, contato_whats@example.com.
    when regexp_contains(
        upper(
            regexp_replace(
                normalize(
                    split({{ texto }}, '@')[safe_offset(0)],
                    NFD
                ),
                r'[^\p{L}0-9]',
                ''
            )
        ),
        r'((TELEFONE|CELULAR|CONTATO).*(ZAP|ZAPZAP|WHATS|WHATSAPP|WPP)|(ZAP|ZAPZAP|WHATS|WHATSAPP|WPP).*(TELEFONE|CELULAR|CONTATO)|^SO(ZAP|ZAPZAP|WHATS|WHATSAPP|WPP).*)'
    )
        then null

    -- Remove e-mails em que o usuário termina com termo de WhatsApp/Zap.
    -- Exemplos: usuario.whatsapp@example.com, usuariozap69@example.com, usuariozap@gmaill.example.
    when regexp_contains(
        upper(
            regexp_replace(
                normalize(
                    split({{ texto }}, '@')[safe_offset(0)],
                    NFD
                ),
                r'[^\p{L}0-9]',
                ''
            )
        ),
        r'(ZAP[0-9]*$|WHATS[0-9]*$|WHATSAPP[0-9]*$|WPP[0-9]*$)'
    )
        then null

    -- Remove e-mails em que a parte antes do @ indica ausência de e-mail
    -- Exemplos: seminformacaodecontato@example.com, estousememail@example.com,
    -- sememailkg@example.com, sememai@example.com.br, sememil@example.com, semimal@example.com.
    when regexp_contains(
        upper(
            regexp_replace(
                normalize(
                    split({{ texto }}, '@')[safe_offset(0)],
                    NFD
                ),
                r'[^\p{L}0-9]',
                ''
            )
        ),
        r'^(ESTOU)?SEM(INFORMACAO|INFORMACOES)?(DE)?CONTATO.*$|^(ESTOU)?SEM(EMAIL|EMAI|EMIL|MAIL|GMAIL|IMAIL|IMAL).*$'
    )
        then null

    -- Remove usuários que indicam ausência de e-mail mesmo quando o termo vem colado com outras palavras ou com erro de digitação.
    -- Exemplos: usuario_naotem@..., pessoa_naotem@..., naotemateagora@...,
    -- naotemnomomento@..., naosabe@..., naoinformado@..., naopossui@...,
    -- emailnaoexiste@..., naoconstaemail@..., naolembraemail@..., usuario_naoconsta@....
    when regexp_contains(
        upper(
            regexp_replace(
                normalize(
                    split({{ texto }}, '@')[safe_offset(0)],
                    NFD
                ),
                r'[^\p{L}0-9]',
                ''
            )
        ),
        r'(NAOTEM|NOATEM|NAOTENHO|NAOSABE|NAOSEI|NAOINFORM|NAOPOSSUI|NAOPOSSU|NAOPOSU|SEMEMAIL|SEMIMAIL|SEMGMAIL|SEMINFORMACAO|SEMINFORMACOES|SEMCONTATO|NAOCONSTA|NAOEXISTE|NAOLEMBRA|NAORELAT|EMAILNAOEXISTE)'
    )
        then null

    -- Remove usuários genéricos antes de qualquer correção de domínio.
    -- Isso evita transformar lixo como placeholder@gamil.example em placeholder@gmail.example.
    when upper(
        regexp_replace(
            normalize(
                split({{ texto }}, '@')[safe_offset(0)],
                NFD
            ),
            r'[^\p{L}0-9]',
            ''
        )
    ) in (
        'NT',
        'NTEM',
        'NAOTEM',
        'NAOTEMNAOTEM',
        'NAOTEMEMAIL',
        'NAOTEMIMAIL',
        'NAOTENHO',
        'NOTEM',
        'NOATEM',
        'NAOTEM23',
        'NAOTE',

        'NP',
        'NPOSSUI',
        'NAOPOSSUI',
        'NAOPOSSUO',
        'NAOPOSSUE',
        'NAOPOSSUIEMAIL',
        'NAOPOSSUIIMAIL',
        'NAOPOSSUICFZA',
        'NOSSUI',

        'NAOINFORMOU',
        'NAOINFORMADO',
        'NAOINFROMADO',
        'NINFORMADO',
        'NAOINFO',
        'NAOINF',
        'EMAILNAOINFORMADO',
        'NAOINFORMA',

        'SEM',
        'SEMINF',
        'SEMINFO',
        'SEMINFORMACAO',
        'SEMINFORMACOES',
        'SEMINFOR',

        'ND',
        'NAODECLARADO',

        'NAO',
        'NAONAO',
        'NENHUM',
        'NAOUSA',
        'NOT',
        'NOEMAIL',
        'NAOSEAPLICA',
        'SEMEMAIL',

        'NAOSEI',
        'IGNORADO',
        'NAOCONSTA',
        'NAOCONSTAEMAIL',
        'EMAILNAOEXISTE',
        'NAOEXISTEEMAIL',
        'NAOLEMBRADOEMAIL',
        'NAOLEMBRAEMAIL',
        'NAOLEMBRAOEMAIL',
        'NAOLEMBRAQ',
        'NAOLEMBRASEUEMAIL',

        'ESTOUSEMEMAILNOMOMENTO',
        'ESTOUSEMEMAIL',
        'ESTOUSEMGMAIL',

        'SEMEMAI',
        'SEMEMIL',
        'SEMIMAL',
        'SEMEMAILKG',
        'SEMINFORMACAODECONTATO',

        '123',
        '123ABC',
        'EU',
        'CF',
        'EMAIL',
        'GMAIL',
        'HOTMAIL',
        'OUTLOOK',
        'YAHOO',
        'TESTE',
        'TESTETESTE',
        'XXXXX',
        'XXXXXX'
    )
        then null

    -- Remove usuários compostos somente por X antes do @, independentemente do domínio.
    -- Exemplos: x@example.com, xx@example.com, xxx@hotmal.example, xx.xx@example.com.
    when regexp_contains(
        upper(
            regexp_replace(
                normalize(
                    split({{ texto }}, '@')[safe_offset(0)],
                    NFD
                ),
                r'[^\p{L}0-9]',
                ''
            )
        ),
        r'^X+$'
    )
        then null

    -- Remove usuários formados por um único caractere repetido 5 ou mais vezes, independentemente do domínio.
    -- Exemplos: aaaaa@example.com, 111111@example.com, mmmmm@example.com.
    when regexp_contains(
        upper(
            regexp_replace(
                normalize(
                    split({{ texto }}, '@')[safe_offset(0)],
                    NFD
                ),
                r'[^\p{L}0-9]',
                ''
            )
        ),
        r'^(A{5,}|B{5,}|C{5,}|D{5,}|E{5,}|F{5,}|G{5,}|H{5,}|I{5,}|J{5,}|K{5,}|L{5,}|M{5,}|N{5,}|O{5,}|P{5,}|Q{5,}|R{5,}|S{5,}|T{5,}|U{5,}|V{5,}|W{5,}|X{5,}|Y{5,}|Z{5,}|0{5,}|1{5,}|2{5,}|3{5,}|4{5,}|5{5,}|6{5,}|7{5,}|8{5,}|9{5,})$'
    )
        then null

    -- Remove valores com até 2 caracteres úteis antes do @ em provedores comuns, incluindo domínios oficiais e principais variações mal digitadas.
    -- Exemplos: a@example.com, a@hotmal.example, ab@gamil.example, j.a@example.com.
    when length(
        regexp_replace(
            normalize(
                trim(split({{ texto }}, '@')[safe_offset(0)]),
                NFD
            ),
            r'[^\p{L}0-9]',
            ''
        )
    ) <= 2
        and (
            trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
                'GMAIL.COM',
                'GMAIL.COM.BR',
                'GMAIL.BR',
                'GMAI.COM',
                'GMIL.COM',
                'GAMIL.COM',
                'GAMAIL.COM',
                'GEMAIL.COM',
                'GEMIL.COM',
                'GMAILL.COM',
                'GMAIIL.COM',
                'GGMAIL.COM',
                'GMAIL.CM',
                'GMAI.CM',
                'GMAIL.CON',
                'GMAI.CON',
                'GMAIL.COMM',
                'GMAIL.COMN',
                'HOTMAIL.COM',
                'HOTMAIL.COM.BR',
                'HOTMAIL.BR',
                'HOTMAI.COM',
                'HOTMAI.COM.BR',
                'HOTMAL.COM',
                'HOTMAL.COM.BR',
                'HOTEMAIL.COM',
                'HOTEMAIL.COM.BR',
                'HOTMAILL.COM',
                'HOTMAIL.CM',
                'HOTMAIL.CON',
                'HOTMAIL.COMM',
                'OUTLOOK.COM',
                'OUTLOOK.COM.BR',
                'OUTLOK.COM',
                'OUTLOK.COM.BR',
                'OTLOOK.COM',
                'OTLOOK.COM.BR',
                'OUTLOOK.CM',
                'OUTLOOK.CON',
                'OUTLOOK.COMM',
                'YAHOO.COM',
                'YAHOO.COM.BR',
                'YAHOO.BR',
                'YAHO.COM',
                'YAHO.COM.BR',
                'YAHOOL.COM',
                'YAHOOL.COM.BR',
                'YAHOOO.COM',
                'YAHOO.CON',
                'YAHOO.CON.BR'
            )
            or regexp_contains(
                trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
                r'(GMAIL|GMAI|GMIL|GAMIL|GAMAIL|GEMAIL|GEMIL|HOTMAIL|HOTMAI|HOTMAL|HOTEMAIL|OUTLOOK|OUTLOK|OTLOOK|YAHOO|YAHO|YAHOOL)'
            )
        )
        then null

    -- Remove valores com um único caractere repetido antes do @ em domínios comuns ou malformados.
    -- Exemplos: aaaa@gamil.example, ddd@gamil.example, xx.xx@example.com, x.xx@example.com.
    when regexp_contains(
        upper(
            regexp_replace(
                normalize(
                    split({{ texto }}, '@')[safe_offset(0)],
                    NFD
                ),
                r'[^\p{L}0-9]',
                ''
            )
        ),
        r'^(A{3,}|B{3,}|C{3,}|D{3,}|E{3,}|F{3,}|G{3,}|H{3,}|I{3,}|J{3,}|K{3,}|L{3,}|M{3,}|N{3,}|O{3,}|P{3,}|Q{3,}|R{3,}|S{3,}|T{3,}|U{3,}|V{3,}|W{3,}|X{3,}|Y{3,}|Z{3,}|0{3,}|1{3,}|2{3,}|3{3,}|4{3,}|5{3,}|6{3,}|7{3,}|8{3,}|9{3,})$'
    )
        and regexp_contains(
            trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
            r'(GMAIL|GMAI|GMIL|GAMIL|GAMAIL|GEMAIL|GEMIL|HOTMAIL|HOTMAI|OUTLOOK|OUTLOK|YAHOO|YAHO)'
        )
        then null

    -- Remove valores com até 2 caracteres úteis antes do @ em domínios comuns ou malformados.
    -- Exemplos: a.m@example.com, ab@gamil.example, c@gmil.example, j.a@example.com.
    when length(
        regexp_replace(
            normalize(
                trim(split({{ texto }}, '@')[safe_offset(0)]),
                NFD
            ),
            r'[^\p{L}0-9]',
            ''
        )
    ) <= 2
        and regexp_contains(
            trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
            r'(GMAIL|GMAI|GMIL|GAMIL|GAMAIL|GEMAIL|GEMIL|HOTMAIL|HOTMAI|OUTLOOK|OUTLOK|YAHOO|YAHO)'
        )
        then null

    -- Remove domínios terminando diretamente em nome de provedor, sem .com/.com.br/.co.uk etc.
    -- Exemplo: telefonezozap@dominio.gmail.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^[A-Z0-9.-]+\.(GMAIL|HOTMAIL|OUTLOOK|YAHOO)$'
    )
        then null

    -- Corrige domínio com hífen digitado antes de provedor conhecido.
    -- Exemplo: usuario@-hotmail.example -> usuario@hotmail.example.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^-+(GMAIL|HOTMAIL|OUTLOOK|YAHOO)\.COM(\.BR)?$'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@',
            regexp_replace(
                trim(lower(split({{ texto }}, '@')[safe_offset(1)])),
                r'^-+',
                ''
            )
        )

    -- Remove formato básico inválido
    when not regexp_contains(
        trim(lower({{ texto }})),
        r'^[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}$'
    )
        then null

    -- Remove e-mails em que a parte antes do @ é composta apenas por números e separadores.
    -- Exemplos: 113@example.com, 1213@example.com, 123.456@example.com, 0@example.com.
    -- Para a base de disparo, esses registros não atendem ao critério mínimo de confiabilidade.
    when regexp_contains(
        trim(split({{ texto }}, '@')[safe_offset(0)]),
        r'^[0-9._%+\-]+$'
    )
        then null

    -- Remove e-mails em que a parte antes do @ é composta apenas por números
    -- e termos de contato, como zap, whatsapp, whats ou wpp.
    -- Exemplos: 27055zap@example.com, zap27055@example.com, 123wpp456@example.com.
    when regexp_contains(
        upper(
            regexp_replace(
                normalize(
                    split({{ texto }}, '@')[safe_offset(0)],
                    NFD
                ),
                r'[^\p{L}0-9]',
                ''
            )
        ),
        r'^[0-9]*(ZAP|ZAPZAP|WHATS|WHATSAPP|WPP)[0-9]*$'
    )
        then null

    -- Remove domínios em que há número/ponto colado antes de provedor conhecido,
    -- inclusive quando o provedor ou o .com está mal digitado.
    -- Exemplos: 2gmail.coom, 54gmail.cim, 75gmail.comm, @4.1gmail.example.
    -- Não é seguro corrigir, porque não dá para saber se o número pertencia ao usuário antes do @.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^[0-9.]+(GMAIL|GMAI|GMAILL|GMIL|GAMIL|GAMAIL|GEMAIL|GEMIL|GGMAIL|GGGMAIL|HOTMAIL|HOTMAI|HOTEMAIL|HOTMAL|OUTLOOK|OUTLOK|OTLOOK|YAHOO|YAHO|YAHOOL|YAHOU)\.(COM|COMM|COMMM|COMMMM|COOM|COOOM|CM|CIM|CPM|CON|OM|CO)(\.BR)?$'
    )
        then null

    -- Remove domínios em que há número colado depois de provedor conhecido.
    -- Exemplos: gmail123.com, hotmail123.com, yahoo123.com.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^(GMAIL|HOTMAIL|OUTLOOK|YAHOO)[0-9]+\.COM(\.BR)?$'
    )
        then null

    -- Corrige Gmail com lixo "x" colado depois do .com.
    -- Exemplo: usuario@gmail.comxxx -> usuario@gmail.com.
    -- Não afeta gmail.com.br porque a regex exige GMAIL.COM seguido apenas de X até o final.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^GMAIL\.COMX+$'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@gmail.com'
        )

    -- Corrige Gmail com apenas uma letra sobrando depois do .com.
    -- Exemplos: gmail.comk, gmail.comf, gmail.coma, gmail.comj, gmail.comp.
    -- Para palavras/nome depois de .com, a regra mais abaixo remove como null.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^GMAIL\.COM[A-Z]$'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@gmail.com'
        )

    -- Corrige domínios com erro de digitação muito provável para Gmail Brasil.
    -- Exemplos: usuario@gmai.cm.br -> usuario@gmail.com.br, usuario@gmail.br -> usuario@gmail.com.br.
    -- Mantém .com.br quando o erro original claramente veio nesse formato.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
        'GMAIL.COM.BR',
        'GMAIL.BR',
        'GMAIL.COM.BE',
        'GMAI.CM.BR',
        'GMAIL.CM.BR',
        'GMAIL.CO.BR',
        'GMAIL.OM.BR',
        'GMAIL.CON.BR',
        'GMAIL.CONM.BR',
        'GMAIL.CPM.BR',
        'GMAIL.COIM.BR',
        'GMAIL.COPM.BR',
        'GMAILCOM.BR',
        'GMAIL.COMBR',
        'GMAI.COM.BR',
        'GMIL.COM.BR',
        'GMIL.CO.BR',
        'GAMIL.COM.BR',
        'GAMAIL.COM.BR',
        'GEMAIL.COM.BR',
        'GEMAIL.COMBR',
        'GEMIL.COM.BR',
        'GMAILMAIL.COM.BR'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@gmail.com.br'
        )

    -- Corrige domínios com erro de digitação muito provável para Gmail.
    -- Exemplos: gmai.com, gmil.com, ggmail.com, gmail.comm, gmail.cm, gmail.con.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
        'GMAI.COM',
        'GMIL.COM',
        'GMAILL.COM',
        'GGMAIL.COM',
        'GGMAILL.COM',
        'GGGMAILL.COM',
        'GGMAIAL.COM',
        'GGMAII.COM',
        'GGMAILD.COM',
        'GMAIIL.COM',
        'GEMAIL.COM',
        'GAMIL.COM',
        'GAMAIL.COM',
        'GEMIL.COM',
        'GMAIL.COMM',
        'GMAIL.COMMM',
        'GMAIL.COMMMM',
        'GMAIL.OM',
        'GMAIL.CM',
        'GMAI.CM',
        'GMAIL.CO',
        'GMAIL.CON',
        'GMAI.CON',
        'GMAIL.CONM',
        'GMAIL.COMN',
        'GMAIL.COMK',
        'GMAIL.COMF',
        'GMAI.COML',
        'GMAIL.CIM',
        'GMAIL.CPM',
        'GMAIL.CCOM',
        'GMAILC.OM',
        'GMAIL.CXOM',
        'GMAIL.COOM',
        'GMAIL.COOOM',
        'GMAIL.OCM',
        'GMAIL.OCOM',
        'GMAIL.OOM',
        'GMAIL.COPM',
        'GMAIL.COIM',
        'GMAIL.CIOM',
        'GMAIL.CPOM',
        'GMAIL.COCM',
        'GMAIL.CVOM',
        'GMAIL.VOM',
        'GMAIL.VCOM',
        'GMAIL.XOM',
        'GMAIL.XCOM',
        'GMAIL.SOM',
        'GMAIL.DOM',
        'GMAIL.BOM',
        'GMAIL.CLOM',
        'GMAIL.COL',
        'GMAIL.CIN',
        'GMAIL.LCOM',
        'GMAIL.COM.COM',
        'GMAIL.COML.COM',
        'GMAIL.COJM',
        'GMAII.COM',
        'GMAIIIL.COM',
        'GMAIILL.COM',
        'GMAIUL.COM',
        'GMAIUL.COMM',
        'GMAIO.COM',
        'GMAIOL.COM',
        'GMAIAL.COM',
        'GMAIM.COM',
        'GMAIML.COM',
        'GMAIML.CO',
        'GMILA.COM',
        'GMAIS.COM',
        'GMAISL.COM',
        'GMAIK.COM',
        'GMAIKL.COM',
        'GMAIEL.COM',
        'GMAIMAIL.COM',
        'GMAI8L.COM',
        'GMAI9L.COM',
        'GMAILS.COM',
        'GMAILA.COM',
        'GMAILK.COM',
        'GMAILM.COM',
        'GMAILC.COM',
        'GMAILX.COM',
        'GMAILO.COM',
        'GMAILLL.COM',
        'GMAILLLL.COM',
        'GMAILL.COOM',
        'GMAIIL.COMMM',
        'GMAIL.GMAIL.COM',
        'GMAIL.GMAIL',
        'GMAIL.MAIL.COM'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@gmail.com'
        )

    -- Corrige Hotmail com apenas uma letra sobrando depois do .com.
    -- Exemplo: hotmail.comk -> hotmail.com.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^HOTMAIL\.COM[A-Z]$'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@hotmail.com'
        )

    -- Corrige domínios com erro de digitação muito provável para Hotmail Brasil.
    -- Exemplo: usuario@hotmai.example.br -> usuario@hotmail.example.br.
    -- Fica separado da regra de Hotmail .com para preservar o .com.br quando o erro já veio com .br.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
        'HOTMAIL.COM.BR',
        'HOTMAIL.BR',
        'HOTMAI.COM.BR',
        'HOTEMAIL.COM.BR',
        'HOTMAL.COM.BR',
        'HOTMAILL.COM.BR',
        'HOTMAIIL.COM.BR',
        'HOTMAIL.COMN.BR',
        'HOTMAIL.COMI.BR',
        'HOTMAIL.CM.BR',
        'HOTMAIL.CO.BR',
        'HOTMAIL.CON.BR',
        'HOTMAIL.OM.BR',
        'HOTMAIL.CIM.BR',
        'HOTMAIL.CPM.BR',
        'HOTMAIL.COPM.BR',
        'HOTMAIL.COOM.BR',
        'HOTMAIL.CCOM.BR',
        'HOTMAIL.COIM.BR'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@hotmail.com.br'
        )

    -- Corrige domínios com erro de digitação muito provável para Hotmail.
    -- Exemplos: hotemail.com, hotmai.com, hotmal.com, hotmail.comm, rotemail.com.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
        'HOTMAI.COM',
        'HOTEMAIL.COM',
        'HOTMAL.COM',
        'HOTMAILL.COM',
        'HOTMAIIL.COM',
        'HOTMAIL.COMM',
        'HOTMAIL.COMMM',
        'HOTMAIL.COMMNMMM',
        'HOTMAIL.COM.COM',
        'HOTMAIL.COMN',
        'HOTMAIL.COMI',
        'HOTMAIL.COMBR',
        'HOTMAIL.CM',
        'HOTMAIL.CO',
        'HOTMAIL.CON',
        'HOTMAIL.OM',
        'HOTMAIL.CIM',
        'HOTMAIL.CPM',
        'HOTMAIL.COPM',
        'HOTMAIL.COOM',
        'HOTMAIL.CCOM',
        'HOTMAIL.COIM',
        'HOTMAIL.XOM',
        'HOTMAILO.COM',
        'HOTMAIO.COM',
        'HOTMAIOL.COM',
        'HOTMAILK.COM',
        'HOTMAILC.COM',
        'HOTMAILC.OM',
        'HOTGMAIL.COM',
        'HOTGMAI.COM',
        'HHOTMAIL.COM',
        'GHOTMAIL.COM',
        'YHOTMAIL.COM',
        'JHOTMAIL.COM',
        'RHOTMAIL.COM',
        'IHOTMAIL.COM',
        'MJHOTMAIL.COM',
        'OOHOTMAIL.COM',
        'HOEMAIL.COM',
        'HOTEMAIL.COMK',
        'ROTEMAIL.COM'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@hotmail.com'
        )

    -- Corrige Outlook com apenas uma letra sobrando depois do .com.
    -- Exemplo: outlook.comk -> outlook.com.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^OUTLOOK\.COM[A-Z]$'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@outlook.com'
        )

    -- Corrige domínios com erro de digitação muito provável para Outlook Brasil.
    -- Exemplo: outlok.com.br -> outlook.com.br.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
        'OUTLOOK.COM.BR',
        'OUTLOK.COM.BR',
        'OTLOOK.COM.BR',
        'OUTLOOK.CO.BR',
        'OUTLOOK.CM.BR',
        'OUTLOOK.CON.BR',
        'OUTLOOK.OM.BR'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@outlook.com.br'
        )

    -- Corrige domínios com erro de digitação muito provável para Outlook.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
        'OTLOOK.COM',
        'OUTLOK.COM',
        'OUTLOOK.CO',
        'OUTLOOK.CON',
        'OUTLOOK.COMM',
        'OUTLOOK.CM',
        'OUTLOOK.OM'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@outlook.com'
        )

    -- Corrige Yahoo com apenas uma letra sobrando depois do .com.
    -- Exemplo: yahoo.comk -> yahoo.com.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^YAHOO\.COM[A-Z]$'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@yahoo.com'
        )

    -- Corrige domínios com erro de digitação muito provável para Yahoo Brasil.
    -- Exemplos: yaho.com.br, yahool.com.br, yahoo.com.be, yahoo.com.nr, yahoo.br.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
        'YAHOO.COM.BR',
        'YAHOO.BR',
        'YAHO.COM.BR',
        'YAHOO.COMBR',
        'YAHOOCOM.BR',
        'YAHOOL.COM.BR',
        'YAHOLL.COM.BR',
        'YAHOOU.COM.BR',
        'YAHOU.COM.BR',
        'YAHOOO.COM.BR',
        'YAHOO.CON.BR',
        'YAHOO.CM.BR',
        'YAHOO.CPM.BR',
        'YAHOO.CIM.BR',
        'YAHOO.CVOM.BR',
        'YAHOO.OM.BR',
        'YAHOO.COMN.BR',
        'YAHOO.COM.BE',
        'YAHOO.COM.BT',
        'YAHOO.COM.NR',
        'YAHOO.COM.BR.BR',
        'YAHOO.CO.BR',
        'YAHO.COM',
        'YAHOL.COM.BR',
        'YAHOU.COM.BR',
        'YAHOOA.COM.BR',
        'YAHOOT.COM.BR',
        'YAHOUL.COM',
        'YAHOLL.COM',
        'YAHOO.COM.COM',
        'YAHOO.OM.BR',
        'YAHOOCOM.BR'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@yahoo.com.br'
        )

    -- Corrige domínios com erro de digitação muito provável para Yahoo.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
        'YAHO.COM',
        'YAHOOO.COM',
        'YAHOOL.COM',
        'YAHOOL.COM.COM',
        'YAHOOU.COM',
        'YAHOLL.COM',
        'YAHOU.COM',
        'YAHOO.CO'
    )
        then concat(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            '@yahoo.com'
        )

    -- Remove domínios com texto/nome colado depois de provedor.com.
    -- Exemplos: gmail.comandrade, gmail.comfilomena, gmail.comcarlos.
    -- Não é seguro cortar esse texto porque pode criar e-mail de outra pessoa.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^(GMAIL|HOTMAIL|OUTLOOK|YAHOO)\.COM[A-Z]{2,}$'
    )
        then null

    -- Remove domínios em que há texto/número colado antes de provedor conhecido.
    -- Exemplos: carlosgmail.com, souza.gmail.com, ferreira.gmail.com, 2021.outlook.com,
    -- leal.yahool.com, machadoyahool.com, fgvgmail.br.
    -- Não é seguro corrigir automaticamente.
    when regexp_contains(
        trim(upper(split({{ texto }}, '@')[safe_offset(1)])),
        r'^[A-Z0-9]+\.?(GMAIL|GMAI|GMAILL|GMIL|GAMIL|GAMAIL|GEMAIL|GEMIL|HOTMAIL|HOTMAI|HOTEMAIL|HOTMAL|OUTLOOK|OUTLOK|OTLOOK|YAHOO|YAHO|YAHOOL|YAHOU)\.(COM|BR)(\.BR)?$'
    )
        and trim(upper(split({{ texto }}, '@')[safe_offset(1)])) not in (
            'GMAIL.COM',
            'GMAIL.COM.BR',
            'HOTMAIL.COM',
            'HOTMAIL.COM.BR',
            'OUTLOOK.COM',
            'OUTLOOK.COM.BR',
            'YAHOO.COM',
            'YAHOO.COM.BR'
        )
        then null

    -- Remove domínios claramente inválidos/suspeitos.
    -- Observação: googlemail.com, zipemail.com.br e globoemail.com não estão nesta lista e seguem válidos.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) in (
        'G.COM',
        'GM.COM',

        'N.COM',
        'NAO.COM',
        'NAO.COM.BR',
        'NAOHA.COM',
        'SEM.COM',
        'SEMEMAIL.COM',

        'SEMGMAIL.COM',
        'SEMGMAIL.COM.BR',
        'SEMEMAI.COM',
        'SEMEMAI.COM.BR',
        'SEMEMIL.COM',
        'SEMEMIL.COM.BR',

        'TEM.COM',
        'NAOTE.COM',
        'NAOTEN.COM',
        'NAOTEM.COM',
        'NAOTEM.CO.BR',
        'NAOTEM.CIM.BR',
        'NAOTEM.COM.BR',
        'NAOTM.COM.BR',
        'NAOTEMEMAIL.COM',
        'NAOTEMEMAIL.COM.BR',
        'NAOTEEMAIL.COM',
        'NAOTEMEMIL.COM',
        'NAOTEMGMAIL.COM',
        'NAOTEM.GMAIL.COM',
        'NAOTEMIMAIL.COM.BR',
        'NAOTEMIMAIL.GMAIL.COM.BR',
        'NAOTEMIMAILGMAIL.COM.BR',
        'NAOTEMINFORMACOES.COM.BR',
        'NAOTEMACESSO.COM',
        'NAOTEMCADASTRO.COM',
        'GNAOTEM.COM',
        'O.TEM',
        'OTEM.COM',
        'OTEM.EMAIL',

        'POSSUI.COM',
        'NAOPOSSUI.COM',
        'NAOPOSSUI.COM.BR',
        'NAOPOSSUIEMAIL.COM',

        'INFORMOU.COM',
        'INFORMADO.COM',
        'NAOINFORMADO.COM',
        'NAOINFORMADA.COM.BR',
        'NAOINFORMADO.COM.BR',
        'NAOINFORMOU.COM',
        'NAOTEMINFORMACAO.COM.BR',
        'GNAOINFMAIO.COM.BR',

        'SEMINFO.COM.BR',
        'SEM.INFORMACAO',
        'SEMINFORMACAO.COM.BR',
        'SEMINFORMACOES.COM.BR',
        'OINFORMOU.COM',

        'NAOEXISTE.COM',
        'INEXISTENTE.COM',

        'NAOSABE.COM',
        'NAOSABE.COM.BR',
        'NAOSABEINFORMAR.COM',
        'NAOSEI.COM',
        'NAOLEMBRA.COM',
        'NAORELATADO.COM',
        'NAOCONSTA.COM',

        'ND.COM.BR',
        'NAO.INF',

        'EMAIL.COM',
        'EMAIL.COM.BR',
        'EMAIL.CM',
        'EMAIL.CONM',
        'EMAIL.COM.BE',
        'EMAILO.COM',

        'TESTE.COM',
        'TESTE.COM.BR',

        '123.COM',
        '123.COM.BR',
        '1234.COM',
        '1234.COM.BR',

        -- Domínios com erro/mistura de Outlook/Hotmail, não confiáveis para disparo.
        'COUTLOOK.COM',
        'COUTLOOK.COM.BR',
        'GOUTLOOK.COM',
        'GOUTLOOK.COM.BR',
        'HOTLOOK.COM',
        'HOTLOOK.COM.BR',
        'HOTLOOK.COMBR',

        -- Domínios com texto colado ou finais suspeitos.
        'GMAIL.CFCARIOCA',
        'GMAIL.COBATISTA',
        'FGVGMAIL.BR',
        'YAHOO.BR.COM',
        'YAHOO.DR',
        'YAHOO.SYCLONE.COM.BR',

        'XX.COM',
        'XX.XX',
        'XXX.XX',
        'XXX.COM',
        'XXXX.COM',
        'XXXXX.COM',
        'XXXXXX.COM',
        'XXXXXXX.COM',
        'XXXXXXXX.COM',
        'XXXXXXXXX.COM',
        'XXXXXXXXXX.COM',
        'XX.XXX',
        'XXX.XXX',
        'XXXX.XXX',
        'XXXXX.XXX',
        'XXXX.XXX.XX',
        'XXX.XX.BR'
    )
        then null

    -- Corrige Gmail com hífen digitado no início do usuário.
    -- Exemplo: -usuario20@gmail.example -> usuario20@gmail.example.
    -- A regra é restrita a gmail.com e só roda depois das regras que removem usuários fake/suspeitos.
    when trim(upper(split({{ texto }}, '@')[safe_offset(1)])) = 'GMAIL.COM'
        and regexp_contains(
            trim(lower(split({{ texto }}, '@')[safe_offset(0)])),
            r'^-+[a-z0-9.]+$'
        )
        and length(regexp_replace(trim(lower(split({{ texto }}, '@')[safe_offset(0)])), r'^-+', '')) > 2
        then concat(
            regexp_replace(trim(lower(split({{ texto }}, '@')[safe_offset(0)])), r'^-+', ''),
            '@gmail.com'
        )

    else trim(lower({{ texto }}))
end
{% endmacro %}
