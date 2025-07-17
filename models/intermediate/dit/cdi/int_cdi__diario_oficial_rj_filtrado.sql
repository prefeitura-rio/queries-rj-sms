{{
    config(
        alias="diario_rj_filtrado",
        materialized="table",
    )
}}

-- Monta tabela base para envio de emails CDI


--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
--=-=-=-=-=-=-=-=-=-=-=-=-=- REGRAS DE FILTRO --=-=-=-=-=-=-=-=-=-=-=-=-=-
--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
-- Aqui montamos quais sao os pontos relevantes do DO segundo regras do CDI

with diarios_municipio as (
    select * from {{ ref('raw_diario_oficial__diarios_rj') }}
),
diarios_municipio_html as (
    select * from {{ ref('raw_diario_oficial__diarios_rj_html') }}
),

-- palavras chave gerais
filtro_palavras_chave as (
  SELECT concat(id_diario,id_materia,secao_indice,bloco_indice,conteudo_indice) as id,* 
  FROM diarios_municipio
  where conteudo like "%Secretaria Municipal%Saúde%"
  or conteudo like "%SMS%"
  or lower(conteudo) like "%clínica da família%"
  or lower(conteudo) like "%policlínica%"
  or lower(conteudo) like "%centros de saúde%"
  or conteudo like "%CAPS %"
  or conteudo like "%UPA %"
  or conteudo like "%CER %"
  or lower(conteudo) like "%hospita%municipa%"
  or lower(conteudo) like "%unidade%saúde%"
  or lower(conteudo) like "%rio%saúde%"
),
-- filtro especifico de retirar exonerações e designações em secretaria municipal
exoneracoes_designacoes as (
    SELECT concat(id_diario,id_materia,secao_indice,bloco_indice,conteudo_indice) as id, 
    concat(id_diario,id_materia,secao_indice,bloco_indice) as id_bloco,
    * 
  FROM diarios_municipio
  where pasta = 'SECRETARIA MUNICIPAL DE SAÚDE/RESOLUÇÕES/RESOLUÇÃO N'
  and (
    lower(conteudo) like "%exoneração%"
    or lower(conteudo) like "%designa%servidores%"
  )
),
-- filtro especifico de retirar plantoes de funeraria
funeraria as (
    SELECT concat(id_diario,id_materia,secao_indice,bloco_indice,conteudo_indice) as id, 
    concat(id_diario,id_materia,secao_indice,bloco_indice) as id_bloco,
    * 
  FROM diarios_municipio
  where pasta = 'AVISOS EDITAIS E TERMOS DE CONTRATOS/SECRETARIA MUNICIPAL DE SAÚDE/AVISOS'
  and (
    lower(conteudo) like "%funerária%"
  )
),
-- filtro especifico para adicionar outros termos em CGM
controladoria_add as (
  select concat(id_diario,id_materia,secao_indice,bloco_indice,conteudo_indice) as id,* 
  FROM diarios_municipio
  where pasta like ('%CONTROLADORIA GERAL DO MUNICÍPIO%')
  and (
    lower(conteudo) like "%art% 167-a da constituição federal%" 
  or lower(conteudo) like "%gestão fiscal do município do rio de janeiro%"
  or lower(conteudo) like "%órgãos e entidades da administração municipal%"
  or lower(conteudo) like "%execução orçamentária%"
  or lower(conteudo) like "%prestação de contas%gestão%"
  )
),
-- filtro especifico para adicionar outros termos em TCM
tribunal_contas_add as (
    select concat(id_diario,id_materia,secao_indice,bloco_indice,conteudo_indice) as id,* 
  FROM diarios_municipio
  where pasta like 'TRIBUNAL DE CONTAS DO MUNICÍPIO%'
  and lower(conteudo) like "%prestação de contas%"
),
-- filtro especifico para retirar despachos e legalidade para fins de registro de TCM e CGM
tribunal_contas_controladoria_del as (
    select concat(id_diario,id_materia,secao_indice,bloco_indice,conteudo_indice) as id, 
    concat(id_diario,id_materia,secao_indice,bloco_indice) as id_bloco 
  FROM diarios_municipio
  where pasta like ('%TRIBUNAL DE CONTAS DO MUNICÍPIO%')
  and (
    lower(conteudo) like "%despacho%"
    or lower(conteudo) like "%legalidade%para fins de registro%"
    or lower(cabecalho) like '%despacho%'
  )
),
-- constroi tabelas com todos as palavras chaves
merge_palavras_chave_add as (
select * from filtro_palavras_chave
union all
select * from controladoria_add
union all
select * from tribunal_contas_add
),
-- retira-se marcações baseando em regras especificas
merge_palavras_chave_del as (
  select id, id_bloco from exoneracoes_designacoes
  union all
  select id, id_bloco from tribunal_contas_controladoria_del
  union all 
  select id, id_bloco from funeraria
),
-- tabela final a partir da qual vamos filtrar o que leva-se para o email
conteudos_para_email as (
select 
  id,  
  data_publicacao,
  id_diario,
  id_materia,
  secao_indice,
  bloco_indice,
  conteudo_indice, 
  pasta,
  arquivo,
  cabecalho, 
  conteudo,
  data_extracao,
  ano_particao,
  mes_particao,
  data_particao
from merge_palavras_chave_add 
where concat(id_diario,id_materia,secao_indice,bloco_indice) not in (select distinct id_bloco from merge_palavras_chave_del)
),
--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
--=-=-=-=-=-=-=-=-=-=-=-=-=- REGRAS DE CAMPOS EM EXPOSICAO --=-=-=-=-=-=-=-=-=-=-=-=-=-
--=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
-- Aqui montamos o que queremos que tenha o email


-- TRIBUNAL DE CONTAS --
final_tcm as (
  select tcm.*, 
    concat(
      assunto.conteudo,
      '\n',
      tcm.conteudo
    )as content_email,
    regexp_extract(tcm.conteudo,r'^[0-9\/]+ ') as voto
  from ( select * from conteudos_para_email where pasta like '%TRIBUNAL DE CONTAS DO MUNICÍPIO%') as tcm
  left join 
  (
    select 
      pasta,
      arquivo,
      cabecalho,
      conteudo, 
      concat(id_diario,id_materia,secao_indice,bloco_indice)  as id_bloco 
    from diarios_municipio
    where conteudo_indice = 0
  ) as assunto
  on id_bloco = concat(tcm.id_diario,tcm.id_materia,tcm.secao_indice,tcm.bloco_indice)
),

-- SECRETARIA DE SAUDE --
avisos_secretaria as (
  select conteudos_para_email.*, 
    concat(
      do_raw.cabecalho,
      '\n',
      do_raw.conteudo
    ) as content_email,
    cast(null as string) as voto
  from conteudos_para_email
  left join 
  (
    select 
      pasta,
      arquivo,
      cabecalho,
      conteudo,
      concat(id_diario,id_materia)  as id_materia_do
    from diarios_municipio
    where bloco_indice = 0
    and secao_indice = 0
  ) as do_raw
  on 
  concat(conteudos_para_email.id_diario,conteudos_para_email.id_materia) = do_raw.id_materia_do
  where conteudos_para_email.pasta like 'AVISOS EDITAIS E TERMOS DE CONTRATOS/SECRETARIA MUNICIPAL DE SAÚDE%'
),
final_secretaria_saude as (
  select tcm.*, 
    cabecalho as content_email,
    cast(null as string) as voto
  from ( select * from conteudos_para_email where pasta like 'SECRETARIA MUNICIPAL DE SAÚDE%') as tcm
  union all
  select * from avisos_secretaria

),
-- ATOS DO PREFEITO --
final_atos_prefeito as (
    select atos.*, 
    concat(
      assunto.cabecalho,
      '\n',
      assunto.conteudo
     ) as content_email,
     cast(null as string) as voto
  from ( select * from conteudos_para_email where pasta  like 'ATOS DO PREFEITO%') as atos
  left join 
  (
    select 
      pasta,
      arquivo,
      cabecalho,
      conteudo,
      concat(id_diario,id_materia)  as id_materia_do
    from diarios_municipio
    where bloco_indice = 0
    and conteudo_indice = 0
    and secao_indice = 0
  ) as assunto
  on id_materia_do = concat(atos.id_diario,atos.id_materia)
),
-- CONTROLADORIA -- 
final_controladoria as (
    select atos.*, 
    concat(
      assunto.cabecalho,
      '\n',
      assunto.conteudo
    ) as content_email,
    cast(null as string) as voto
  from ( select * from conteudos_para_email where pasta  like '%CONTROLADORIA%') as atos
  left join
  (
    select 
      pasta,
      arquivo,
      cabecalho,
      conteudo,
      concat(id_diario,id_materia)  as id_materia_do
    from diarios_municipio
    where bloco_indice = 0
    and conteudo_indice = 0
    and secao_indice = 0
  ) as assunto
  on id_materia_do = concat(atos.id_diario,atos.id_materia)
),
final_all_sections as (
    select * from final_atos_prefeito
    union all 
    select * from final_tcm
    union all 
    select * from final_controladoria
    union all 
    select * from final_secretaria_saude
)

select 
    final_all_sections.* except(data_extracao,ano_particao,mes_particao,data_particao,id),
    html.html as html,
    final_all_sections.data_extracao,
    final_all_sections.ano_particao,
    final_all_sections.mes_particao,
    final_all_sections.data_particao
from final_all_sections
left join diarios_municipio_html as html
on concat(final_all_sections.id_diario,final_all_sections.id_materia) = concat(html.id_diario,html.id_materia)





