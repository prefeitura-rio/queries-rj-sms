{% macro clean_bairro(val) %}
case
  -- Nulos comuns ("None", "NULL", etc)
  when {{ process_null(val) }} is null
    then null

  -- Sinônimos de nulo
  when lower(trim({{ val }})) in (
    "ignorado",
    "ser alterado",
    "nenhum",
    "indefinido"
  )
    then null

  -- Cidades
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "belforoxo", "belforroxo", "belfordroxo",
    "cabofrio",
    "cachoeirasdemacacu", "cachoeirademacacu",
    "duquedecaxias",
    "itaborai",
    "itaguai",
    "macae",
    "mage",
    "marica",
    "niteroi",
    "nilopolis", "nilopoles",
    "novaiguacu",
    "petropolis",
    "queimados",
    "saogoncalo",
    "saojoaodemeriti", "saojoaodemiriti",
    "seropedica",
    "teresopolis", "terezopolis"
  )
    then null

  -- Textos que com 0 ou 1 letra somente
  when length(
    REGEXP_REPLACE(
      lower(normalize({{ val }}, nfd)),
      r"[^a-z]",
      ""
    )
  ) <= 1  -- tamanhos >=2 permitem ainda 'CG' p.ex.
    then null


  -----------------------
  -- Algumas variações de bairros cariocas hardcoded

  -- Abolição
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "abolicao"
    then "Abolição"

  -- Acari

  -- Água Santa
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "aguasanta"
    then "Água Santa"

  -- Alto da Boa Vista
  -- Argentino
  -- Anchieta

  -- Andaraí
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "andarai"
    then "Andaraí"

  -- Anil

  -- Bancários
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "bancarios"
    then "Bancários"

  -- Bangu

  -- Barra da Tijuca
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "bdatijuca", "barradatijuca"
  )
    then "Barra da Tijuca"

  -- Barra de Guaratiba
  -- Barros Filho
  -- Benfica
  -- Bento Ribeiro
  -- Bonsucesso

  -- Botafogo
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "botafogo",
    "botagofo"  -- juro
  )
    then "Botafogo"

  -- Brás de Pina
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "brasdepina", "brazdepina"
  )
    then "Brás de Pina"

  -- Cachambi
  -- Cacuia
  -- Caju

  -- Camorim
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "camorim",
    "camorimjacarepagua"
  )
    then "Camorim"

  -- Campinho

  -- Campo dos Afonsos
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "campoafonsos", "campodeafonsos", "campodosafonsos"
  )
    then "Campo dos Afonsos"

  -- Campo Grande
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "cg", "cgrande", "campogrande",
    "campogrnde"
  )
    then "Campo Grande"

  -- Cascadura
  -- Catete
  -- Catumbi
  -- Cavalcanti

  -- Centro
  when
    -- Pode não ser Centro do Rio; costumam colocar "Centro (cidade)"
    -- ou "Centro - Cidade" nesses casos
    REGEXP_CONTAINS(
      lower(normalize({{ val }}, nfd)),
      r"^centro\s*([\-\(]|$)"
    )
    -- Bairro de Fátima é sub-bairro no Centro
    or (REGEXP_REPLACE(
      lower(normalize({{ val }}, nfd)),
      r"[^a-z]",
      ""
    ) in (
      "fatima",
      "bairrodefatima",
      "bairrodefatimacentro"
    ))
    then "Centro"

  -- Cidade de Deus
  -- Cidade Nova

  -- Cidade Universitária
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "cidadeuniversitaria"
    then "Cidade Universitária"

  -- Cocotá
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "cocota",
    "cocotailha",
    "cocotailhadogovernador"
  )
    then "Cocotá"

  -- Coelho Neto

  -- Colégio
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "colegio"
    then "Colégio"

  -- Complexo do Alemão
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "complexodoalemao"
    then "Complexo do Alemão"

  -- Copacabana
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "copc",
    "copabana",
    "copacabana"
  )
    then "Copacabana"

  -- Cordovil
  -- Cosme Velho

  -- Cosmos
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "cosmo",
    "cosmos"
  )
    then "Cosmos"

  -- Costa Barros

  -- Curicica
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "curicica",
    "curicicajacarepagua"
  )
    then "Curicica"

  -- Del Castilho
  -- Deodoro
  -- Encantado
  -- Engenheiro Leal
  -- Engenho da Rainha
  -- Engenho de Dentro
  -- Engenho Novo

  -- Estácio
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "estacio"
    then "Estácio"

  -- Flamengo

  -- Freguesia (ilha)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "freguesiailha",
    "ilhafreguesia"
  ) 
    then "Freguesia (Ilha)"

  -- Freguesia (Jacarepaguá)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "freguesiajacarepagua"
  ) 
    then "Freguesia (Jacarepaguá)"

  -- Galeão
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "galeao"
    then "Galeão"

  -- Gamboa

  -- Gardênia Azul
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "gardeniaazul"
    then "Gardênia Azul"

  -- Gávea
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "gavea"
    then "Gávea"

  -- Gericinó
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "gericino"
    then "Gericinó"

  -- Glória
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "gloria"
    then "Glória"

  -- Grajaú
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "grajau"
    then "Grajaú"

  -- Grumari
  -- Guadalupe

  -- Guaratiba
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "guaratiba", "guaratibaba"
  )
    then "Guaratiba"

  -- Higienópolis
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "higienopolis"
    then "Higienópolis"

  -- Honório Gurgel
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "honoriogurgel"
    then "Honório Gurgel"

  -- Humaitá
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "humaita"
    then "Humaitá"

  -- Ilha de Guaratiba

  -- Inhaúma
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "inhauma"
    then "Inhaúma"

  -- Inhoaíba
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "inhoaiba"
    then "Inhoaíba"

  -- Ipanema

  -- Irajá
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "iraja"
    then "Irajá"

  -- Itanhangá
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "itanhanga"
    then "Itanhangá"

  -- Jabour

  -- Jacaré
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "jacare"
    then "Jacaré"

  -- Jacarepaguá
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "jacarepagua"
    then "Jacarepaguá"

  -- Jacarezinho

  -- Jardim América
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "jdamerica",
    "jardimamerica"
  )
    then "Jardim América"

  -- Jardim Botânico
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "jdbotanico",
    "jardimbotanico"
  )
    then "Jardim Botânico"

  -- Jardim Carioca
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "jdcarioca",
    "jardimcarioca",
    "jardimcariocailha",
    "jardimcariocacocotailhadogovern" -- ;s
  )
    then "Jardim Carioca"

  -- Jardim Guanabara
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "jdguanabara",
    "jardimguanabara"
  )
    then "Jardim Guanabara"

  -- Jardim Sulacap
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "jdsulacap",
    "jardimsulacap"
  )
    then "Jardim Sulacap"

  -- Joá
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "joa"
    then "Joá"

  -- Lagoa
  -- Lapa
  -- Laranjeiras
  -- Leblon
  -- Leme

  -- Lins de Vasconcelos
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "linsvasconcelos", "linsdevasconcelos"
  )
    then "Lins de Vasconcelos"

  -- Madureira
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "madureira", "madureirar"
  )
    then "Madureira"

  -- Magalhães Bastos
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "magalhaesbastos"
    then "Magalhães Bastos"

  -- Mangueira
  -- Manguinhos

  -- Maracanã
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "maracana"
    then "Maracanã"

  -- Maré
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "mare",
    "viladojoaomare"
  )
    then "Maré"

  -- Marechal Hermes

  -- Maria da Graça
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "mdagraca", "mariadagraca"
  )
    then "Maria da Graça"

  -- Méier
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "meier"
    then "Méier"

  -- Moneró
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "monero"
    then "Moneró"

  -- Olaria

  -- Osvaldo Cruz
  -- Datario usa V no nome: https://www.data.rio/datasets/PCRJ::limite-de-bairros/explore
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "oswadocruz",
    "oswaldocruz", "oswaldocrus",
    "osvaldocruz", "osvaldocrus"
  )
    then "Osvaldo Cruz"

  -- Paciência
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "paciencia"
    then "Paciência"

  -- Padre Miguel

  -- Paquetá
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "paqueta"
    then "Paquetá"

  -- Parada de Lucas
  -- Parque Anchieta

  -- Parque Colúmbia
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "pqcolumbia", "parquecolumbia"
  )
    then "Parque Colúmbia"

  -- Pavuna

  -- Pechincha
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "pechincha", "pexincha", "pechinxa", "pexinxa",
    "pechinchajacarepagua"
  )
    then "Pechincha"

  -- Pedra de Guaratiba
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "pedraguaratiba", "pedradeguaratiba"
  )
    then "Pedra de Guaratiba"

  -- Penha
  -- Penha Circular
  -- Piedade
  -- Pilares
  -- Pitangueiras

  -- Portuguesa
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "portuguesa",
    "ilhaportuguesa",
    "ilhadogovernadorportuguesa",
    "ilhadogorvernadorportuguesa",
    "portuguesailha"
  )
    then "Portuguesa"

  -- Praça da Bandeira
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "pbandeira", "pcbandeira", "pcabandeira", "pracabandeira",
    "pdabandeira", "pcdabandeira", "pcadabandeira", "pracadabandeira"
  )
    then "Praça da Bandeira"

  -- Praça Seca
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "pseca", "pcseca", "pcaseca", "pracaseca"
  )
    then "Praça Seca"

  -- Praia da Bandeira

  -- Quintino Bocaiúva
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "qbocaiuva", "quintino", "quintinobocaiuva",
    "qbocaiuvademelo" --??
  )
    then "Quintino Bocaiúva"

  -- Ramos

  -- Realengo
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "realendo", "realengo"
  )
    then "Realengo"

  -- Recreio dos Bandeirantes
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "rec", "recreio",
    "recreiobanderantes", "recreiobandeirantes",
    "recreiodebanderantes", "recreiodosbanderantes",
    "recreiodosbandeira",
    "recreiodebandeirantes", "recreiodosbandeirantes"
  )
    then "Recreio dos Bandeirantes"

  -- Riachuelo
  -- Ribeira
  -- Ricardo de Albuquerque

  -- Rio Comprido
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "rcomprido", "riocomprido",
    "rcumprido", "riocumprido"
  )
    then "Rio Comprido"

  -- Rocha
  -- Rocha Miranda
  -- Rocinha
  -- Sampaio

  -- Santa Cruz
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "scruz", "stcruz", "stacruz", "santacruz",
    "scrus", "stcrus", "stacrus", "santacrus"
  )
    then "Santa Cruz"

  -- Santa Teresa
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "steresa", "stteresa", "stateresa", "santateresa",
    "stereza", "sttereza", "statereza", "santatereza"
  )
    then "Santa Teresa"

  -- Santíssimo
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "santissimo"
    then "Santíssimo"

  -- Santo Cristo
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "scristo", "stcristo", "stocristo", "santocristo"
  )
    then "Santo Cristo"

  -- São Conrado
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "sconrado", "saoconrado"
  ) 
    then "São Conrado"

  -- (Imperial de) São Cristóvão
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "scristovao", "saocristovao",
    "iscristovao",
    "idescristovao",
    "imperialdescristovao",
    "imperialdesaocristovao"
  ) 
    then "Imperial de São Cristóvão"

  -- São Francisco Xavier
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "sfx", "sfcox",
    "sfcoxavier", "saofcox",
    "saofcoxavier",
    "sfranciscoxavier",
    "saofranciscoxavier"
  )
    then "São Francisco Xavier"

  -- Saúde
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "saude"
    then "Saúde"

  -- Senador Camará
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "scamara", "sencamara", "senadorcamara"
  )
    then "Senador Camará"

  -- Senador Vasconcelos
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "svasconcelos", "senvasconcelos", "senadorvasconcelos"
  )
    then "Senador Vasconcelos"

  -- Sepetiba
  -- Tanque
  -- Taquara

  -- Tauá
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "taua", "tauailha"
  )
    then "Tauá"

  -- Tijuca
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "tijuca", "tijiuca",
    -- Barão de Itapagiipe é uma rua parcialmente do Rio Comprido,
    -- mas majoritariamente Tijucana
    "baraodeitapagiipe"
  )
    then "Tijuca"

  -- Todos os Santos
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "tsantos", "tdssantos", "tdsossantos",
    "todossantos", "todosossantos"
  )
    then "Todos os Santos"
  
  -- Tomás Coelho
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "tomascoelho", "tomazcoelho"
  )
    then "Tomás Coelho"

  -- Turiaçú
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "turiacu"
    then "Turiaçú"

  -- Urca

  -- Vargem Grande
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "vgd", "vgrande", "vggrande", "vgmgrande",
    "vrggrande",  "vargemgrande",
    "vargengrande", "varjemgrande"
  )
    then "Vargem Grande"

  -- Vargem Pequena
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "vpq", "vpeq", "vpeqn", "vpequena",
    "vgpequena", "vgmpequena",
    "vrgpequena",  "vargempequena",
    "vargenpequena", "varjempequena"
  )
    then "Vargem Pequena"

  -- Vasco da Gama
  -- Vaz Lobo
  -- Vicente de Carvalho
  -- Vidigal

  -- Vigário Geral
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "vigariogeral"
    then "Vigário Geral"

  -- Vila da Penha
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "vpenha", "vlpenha",
    "vdapenha",  "vldapenha", "viladapenha"
  )
    then "Vila da Penha"

  -- Vila Isabel
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "visabel", "vlisabel", "vilaisabel", "vilaisabell",
    "vizabel", "vlizabel", "vilaizabel", "vilaizabell"
  )
    then "Vila Isabel"

  -- Vila Kennedy
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "vkened",   "vlkened",   "vilakened",
    "vkenedy",  "vlkenedy",  "vilakenedy",
    "vkenedi",  "vlkenedi",  "vilakenedi",
    "vkenned",  "vlkenned",  "vilakenned",
    "vkennedy", "vlkennedy", "vilakennedy",
    "vkennedi", "vlkennedi", "vilakennedi"
  )
    then "Vila Kennedy"

  -- Vila Kosmos
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "vkosmos", "vlkosmos", "vilakosmos",
    "vcosmos", "vlcosmos", "vilacosmos"
  )
    then "Vila Kosmos"

  -- Vila Militar
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "vlmilitar", "vilamilitar"
  )
    then "Vila Militar"

  -- Vila Valqueire
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "valqueire", "vlvalqueire", "vilavalqueire"
  )
    then "Vila Valqueire"

  -- Vista Alegre
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "valegre", "vistaalegre"
  )
    then "Vista Alegre"

  -- Zumbi


  -----------------------
  -- Bairros de outras cidades

  -- Agostinho Porto (S. J. de Meriti)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "aporto", "agporto", "agostinhoporto"
  )
    then "Agostinho Porto"

  -- Amapá (Duque de Caxias)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "amapa", "duquedecaxiasamapa"
  )
    then "Amapá"

  -- Chaperó (Itaguaí)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "chapero"
    then "Chaperó"

  -- Éden (S. J. de Meriti)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "eden", "edem"
  )
    then "Éden"

  -- Icaraí (Niterói)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "icarai"
    then "Icaraí"

  -- Paraíso (São Gonçalo / Nova Iguaçu)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "paraiso"
    then "Paraíso"

  -- Piabetá (Magé)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "piabeta"
    then "Piabetá"

  -- Xerém (Duque de Caxias)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) = "xerem"
    then "Xerém"


  -----------------------

  -- "Ilha do Governador" (não é um bairro mas tem um monte)
  when REGEXP_REPLACE(
    lower(normalize({{ val }}, nfd)),
    r"[^a-z]",
    ""
  ) in (
    "ilhadogovernado",
    "ilhadogovernador",
    "ilhadogorvernador"
  )
    then "Ilha do Governador"

  -- Outros
  else {{ proper_br(add_tilde_to_saints(val)) }}
end
{% endmacro %}
