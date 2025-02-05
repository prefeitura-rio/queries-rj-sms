{{
    config(
        alias="ficha_a",
        materialized="table",
    )
}}

-- dbt run --select raw_prontuario_vitacare__paciente
with
    ficha_a as (
        select *, 'rotineiro' as tipo,
        from {{ ref("base_prontuario_vitacare__ficha_a_rotineiro") }}
        union all
        select *, 'historico' as tipo,
        from {{ ref("base_prontuario_vitacare__ficha_a_historico") }}
    ),
    -- -----------------------------------------------------
    -- Padronização
    -- -----------------------------------------------------
    ficha_a_padronizada as (
        select 
            safe_cast(cpf as string) as cpf,
            REGEXP_REPLACE(CAST(id_paciente AS STRING), '\x00', '') AS id_paciente,
            numero_prontuario,
            safe_cast(unidade_cadastro as string) as unidade_cadastro,
            regexp_replace(ap_cadastro,r'\.0','') as ap_cadastro,
            {{ proper_br('nome') }} as nome,
            case 
                when sexo = 'male' then 'masculino'
                when sexo = 'female' then 'feminino'
                else null
            end as sexo,
            case 
                when obito in ('1','True') then true 
                when obito in ('0','False') then false 
                else null
            end as obito,
            safe_cast(bairro as string) as bairro,
            safe_cast(comodos as integer) as comodos,
            case 
                when regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                trim(lower(nome_mae)),
                                r'(ausente)|(pai desconhecido)|(desconhecid[ao])|(desconhece)|(ignorad[oa])|(^x+$)|-|\?',
                                ''),
                            r'sem {0,1}(nome|registro|informa{0,1}[cçãa][aã]{0,1}(oes|ões|o)|info{0,1}|identifica[cç][aã]o){0,1}',
                            ''
                            ),
                            r'(nada|n[aã]{0,1}o{0,1}) {0,1}(possui|informad[ao]|informou|inf|consta|declarad[ao]|tem|identificad[ao]){0,1}',
                            ''
                         ) = ''  then null 
                else {{ proper_br('nome_mae') }} 
            end as nome_mae,
            case 
                when regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                trim(lower(nome_pai)),
                                r'(ausente)|(pai desconhecido)|(desconhecid[ao])|(desconhece)|(ignorad[oa])|(^x+$)|-|\?',
                                ''),
                            r'sem {0,1}(nome|registro|informa{0,1}[cçãa][aã]{0,1}(oes|ões|o)|info{0,1}|identifica[cç][aã]o){0,1}',
                            ''
                            ),
                            r'(nada|n[aã]{0,1}o{0,1}) {0,1}(possui|informad[ao]|informou|inf|consta|declarad[ao]|tem|identificad[ao]){0,1}',
                            ''
                         ) = ''  then null else {{ proper_br('nome_pai') }} 
            end as nome_pai,
            case
                when lower(raca_cor) = "sim" then null
                when lower(raca_cor) = "não" then null
                else lower(raca_cor) 
            end as raca_cor,
            safe_cast(ocupacao as string) as ocupacao,
            case 
                when lower(religiao) not in ('sem religião','católica','outra',
                'evangélica','espírita','religião de matriz africana','budismo',
                'judaísmo','islamismo','candomblé') then null
                else religiao
            end as religiao,
            {{ padronize_telefone('telefone') }} as telefone,
            safe_cast(ine_equipe as string) as ine_equipe,
            safe_cast(microarea as string) as microarea,
            nullif(regexp_replace(regexp_replace(logradouro,'^0.*$',''),'null',''),'') as logradouro,
            case 
                when lower(nome_social) in ('sem informacao','fora do territorio',
                'fora de area','nao inf','plano empresa','','nao possui','mudou se','plano individual') then null
                else {{ proper_br('nome_social') }}
            end  as nome_social,
            case 
                when lower(destino_lixo) not in ('coletado','céu aberto','outro',
                'queimado/enterrado') then null 
                else destino_lixo
            end as destino_lixo,
            case 
                when luz_eletrica in ('1','True') then true 
                when luz_eletrica in ('0','False') then false 
                else null
            end as luz_eletrica,
            safe_cast(codigo_equipe as string) as codigo_equipe,
            timestamp_sub(timestamp(data_cadastro, "Brazil/East"),interval 2 hour) as data_cadastro,
            case 
                when lower(escolaridade) not in ('médio completo','fundamental incompleto',
                'fundamental completo','alfabetizado','médio incompleto','superior completo',
                'superior incompleto','especialização/residência','mestrado','doutorado',
                '4º ano/3ª série') then null
                else escolaridade
            end as escolaridade,
            regexp_replace(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(lower(tempo_moradia),r'^\+ de','mais de '),
                        r'^\+','mais de '
                    ),
                    ' {2,}',
                    ' '
                ),
                '[.,-]',
                ''
            ) as tempo_moradia,
            lower(nacionalidade) as nacionalidade,
            case 
                when lower(renda_familiar) not in ('1 salário mínimo','2 salários mínimos',
                '3 salários mínimos','1/2 salário mínimo','1/4 salário mínimo',
                '4 salários mínimos','mais de 4 salários mínimos') then null
                else renda_familiar
            end as renda_familiar,
            case 
                when lower(tipo_domicilio) not in ('alvenaria/tijolo','alvenaria/tijolo com revestimento',
                'alvenaria/tijolo sem revestimento','outros','material aproveitado',
                'taipa revestida','madeira','taipa não revestida') then null
                else tipo_domicilio
            end as tipo_domicilio,-- ver com poli preenchimentos possiveis
            safe_cast(data_nascimento as date) as data_nascimento,
            safe_cast(initcap(pais_nascimento) as string) as pais_nascimento,
            safe_cast(initcap(tipo_logradouro) as string) as tipo_logradouro,
            case 
                when lower(tratamento_agua) not in ('filtração','cloração','sem tratamento',
                'mineral','fervura') then null
                else tratamento_agua
            end as tratamento_agua,
            case 
                when em_situacao_de_rua in ('1','True') then true 
                when em_situacao_de_rua in ('0','False') then false 
                else null
            end as em_situacao_de_rua,
            case 
                when frequenta_escola = '1' then true
                when frequenta_escola = '0' then false
                else null
            end as frequenta_escola,
            split(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(trim(meios_transporte),r'[\[|\]|"]',''),
                        r"[\']",
                        ''
                    ),
                    r', ',
                    r','
                ),
                ','
            ) as meios_transporte,
            safe_cast(situacao_usuario as string) as situacao_usuario,
            split(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(trim(doencas_condicoes),r'[\[|\]|"]',''),
                        r"[\']",
                        ''
                    ),
                    r', ',
                    ','
                ),
                ','
            ) as doencas_condicoes,
            nullif({{clean_name_string('estado_nascimento')}},'') as estado_nascimento, 
            nullif({{clean_name_string('estado_residencia')}},'') as estado_residencia,
            case 
                when lower(identidade_genero) not in ('cis','mulher transexual',
                'homem transexual','outro') then null
                else identidade_genero
            end as identidade_genero,
            split(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(trim(meios_comunicacao),r'[\[|\]|"]',''),
                        r"[\']",
                        ''
                    ),
                    r', ',
                    ','
                ),
                ','
            ) as meios_comunicacao,
            case 
                when lower(orientacao_sexual) not in ('heterossexual','homossexual (gay / lésbica)',
                'outro','bissexual') then null
                else orientacao_sexual
            end as orientacao_sexual,
            case 
                when possui_filtro_agua in ('1','True') then true 
                when possui_filtro_agua in ('0','False') then false 
                else null
            end as possui_filtro_agua,
            case 
                when possui_plano_saude in ('1','True') then true 
                when possui_plano_saude in ('0','False') then false 
                else null
            end as possui_plano_saude,
            case 
                when lower(situacao_familiar) not in ('convive com familiar(es), sem companheira(o)',
                'vive com companheira(o) e filho(s)',
                'vive só','Convive com companheira(o) com laços conjugais e sem filhos',
                'convive com companheira(o) com filhos e/ou outro(s) familiar(es)',
                'convive com outras pessoas sem laços consanguíneos e/ou conjugais',
                'sem informações') then null
                else situacao_familiar
            end as situacao_familiar,
            case 
                when territorio_social in ('1','True') then true 
                when territorio_social in ('0','False') then false 
                else null
            end as territorio_social,
            case 
                when lower(abastecimento_agua) not in ('rede pública','poço ou nascente','outro',
                'cisterna','carro pipa') then null
                else abastecimento_agua
            end as abastecimento_agua,
            case 
                when animais_no_domicilio in ('1','True') then true 
                when animais_no_domicilio in ('0','False') then false 
                else null
            end as animais_no_domicilio,
            case 
                when cadastro_permanente in ('1','True') then true 
                when cadastro_permanente in ('0','False') then false 
                else null
            end as cadastro_permanente,
            case 
                when lower(familia_localizacao) not in ('urbana','rural') then null
                else familia_localizacao
            end as familia_localizacao,
            split(
                regexp_replace(
                    regexp_replace(
                        regexp_replace(trim(em_caso_doenca_procura),r'[\[|\]|"]',''),
                        "[\']",
                        ''
                    ),
                    r', ',
                    r','
                ),
                ','
            ) as em_caso_doenca_procura,
            -- trazer join com municipio de nascimento para termos codigos em ambos
            struct(
                case 
                    when regexp_contains(municipio_nascimento,'[0-9]') 
                        and municipio_nascimento != '-1' then municipio_nascimento
                    else null
                end as codigo,
                case 
                    when regexp_contains(municipio_nascimento,'[A-Za-a]') then municipio_nascimento
                    else null
                end as nome
            ) as municipio_nascimento,
            struct(
                regexp_extract(municipio_residencia,r'\[IBGE: ([0-9]{1,9})\]') as codigo,
                trim(regexp_replace(municipio_residencia,r'\[IBGE: ([0-9]{1,9})\]','')) as nome
             ) as municipio_residencia,
            case 
                when responsavel_familiar in ('1','True') then true 
                when responsavel_familiar in ('0','False') then false 
                else null                
            end as responsavel_familiar,
            case 
                when lower(esgotamento_sanitario) not in ('sistema de esgoto (rede)','fossa','ceu aberto',
                'direto para rio/lago/mar') then null
                else esgotamento_sanitario
            end as esgotamento_sanitario,
            case
                when lower(situacao_moradia_posse) not in ('próprio','alugado','outra','cedido','financiado',
                'ocupação','instituição de permanência','situação de rua','arrendado') then null
                else situacao_moradia_posse
            end as situacao_moradia_posse,
            case 
                when lower(situacao_profissional) not in ('emprego formal','não se aplica','desempregado',
                'emprego informal','pensionista / aposentado','autônomo','outro','não trabalha','empregador',
                'autônomo sem previdência social','autônomo com previdência social') then null 
                else situacao_profissional
            end as situacao_profissional,
            case 
                when vulnerabilidade_social in ('1','True') then true
                when vulnerabilidade_social in ('0','False') then false
                else null
            end as vulnerabilidade_social,
            case 
                when familia_beneficiaria_cfc in ('1','True') then true
                when familia_beneficiaria_cfc in ('0','False') then false 
                else null
            end as familia_beneficiaria_cfc,
            timestamp_sub(timestamp(data_atualizacao_cadastro, "Brazil/East"),interval 2 hour) as data_atualizacao_cadastro,
            case 
                when participa_grupo_comunitario in ('1','True') then true
                when participa_grupo_comunitario in ('0','False') then false 
                else null
            end as participa_grupo_comunitario,
            case 
                when lower(relacao_responsavel_familiar) not in ('filho(a)','cônjuge/companheiro(a)','outro parente',
                'não parente','pai/mãe','neto(a)/bisneto(a)','irmão/irmã','genro/nora','enteado(a)','sogro(a)') then null 
                else relacao_responsavel_familiar
            end as relacao_responsavel_familiar,
            case 
                when membro_comunidade_tradicional in ('1','True') then true
                when membro_comunidade_tradicional in ('0','False') then false 
                else null
            end as membro_comunidade_tradicional,
            timestamp_sub(timestamp(data_atualizacao_vinculo_equipe, "Brazil/East"),interval 2 hour) as data_atualizacao_vinculo_equipe,
            case 
                when familia_beneficiaria_auxilio_brasil in ('1','True') then true
                when familia_beneficiaria_auxilio_brasil in ('0','False') then false
                else null
            end as familia_beneficiaria_auxilio_brasil,
            case 
                when crianca_matriculada_creche_pre_escola in ('1','True') then true 
                when crianca_matriculada_creche_pre_escola in ('0','False') then false 
                else null
            end as crianca_matriculada_creche_pre_escola,
            updated_at,
            loaded_at,
            tipo

        from ficha_a
    )

select *
from ficha_a_padronizada
