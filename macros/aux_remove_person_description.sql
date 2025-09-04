{% macro aux_remove_person_description(text) %}
    -- Ferramenta boa pra testar: https://regexr.com/
    -- (você vai precisar de exemplos)

    -- Aqui eliminamos o início de entradas que começam com
    -- "Homem / Mulher (incluindo 'home', 'homen')", e depois seguem com
    -- qualquer quantidade das seguintes palavras:
    -- > Pard@, Negr@, Branc@, Moren@, Pret@
    -- > Jovem, Adult@
    -- > Alt@, Median@, Medi@, Baix@, Magr@, Gord@, Barbud@
    -- > Alegad@
    -- > Desconhecid@, Balead@
    -- > Trans
    -- > Morador(a) de rua
    -- > "HOJE" (quando não tem nome, às vezes é "homem hoje" ou variantes)
    -- > "ESTATURA" (ex. "baixa estatura")
    -- > "SEM NOME"
    -- > "UM", "DOIS" (ex. "homem pardo dois")
    -- > "APARENTANDO TER"
    -- > "MAIS", "OU", "MENOS",
    --   (qualquer número por extenso entre 10 e 90)
    --   ("E __") "ANOS") (ex. "mais ou menos trinta e cinco anos"; "vinte anos"; etc)
    --   - Também remove datas (ex. "vinte e dois de outubro")
    -- > Não identificado/sem identificação
    -- > Declara/ou ser,
    -- > Identificado/conhecido como
    -- > Parênteses
    -- Não é uma lógica infalível! Muito texto muito específico:
    -- Datas, comentários sobre aparência, tatuagens, mas também nomes, apelidos, ...
    REGEXP_REPLACE(
        TRIM({{ text }}),
        r'(?i)^(HOME(M|N)?|MULHER)\b(\s|(PAR(D|T)|NEGR|BRANC|MOREN|PRET|JOVEM|ADULT|A(L|U)T|MEDIAN|MEDI|BAIX|MAGR|GORD|BARBUD|ALEGAD|DESCONHECID|BALEAD)(O|A)?|TRANS|MORADORA? DE RUA|HOJE|ESTATURA|SEM NOME|UM|DOIS|APARENTA(NDO)?( TER)?|MAIS|\+|OU|\/|\\|MENOS|\-|(DEZ[A-Z]*|(ON|DOU?|TRE|(CA|QUA)TOR|QUIN)ZE|VINTE|(TR?I|QUARE|CINQUE|SESS?E|SETE|OITE|NOVE)NTA)?(\s*E\s+[A-Z]+)?\s*(ANOS?|DE [A-Z]+\b)|(N(Ã|A)O|SEM) ID[^\s]+|DECLAR(ADO|OU|A)( SER)?|(IDENTIFICAD|CONHECID)(O|A)? COMO|[\!-\@´`~\^\[\]_\{\}\|])*',
        ''
    )
{% endmacro %}
