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
            safe_cast(id_paciente as string) as id_paciente,
            safe_cast(unidade_cadastro as string) as unidade_cadastro,
            regexp_replace(ap_cadastro,r'\.0','') as ap_cadastro,
            {{ proper_br('nome') }} as nome,
            case 
                when sexo = 'male' then 'masculino'
                when sexo = 'female' then 'feminino'
                else null
            end as sexo,
            safe_cast(obito as bool) as obito,
            safe_cast(bairro as string) as bairro,
            safe_cast(comodos as integer) as comodos,
            case 
                when regexp_replace(lower(nome_mae),r'(sem registro)|(sem informa[c|ç|ã][a|ã|][o|(oes)|(ões)])|(m[ã|a]e desconhecida)|(n[a|ã|][|o] informado)|(n[a|ã]o declarado)|(desconhecido)|(n[a|ã]o declarado)|(n[a|ã]o consta)|(sem inf[|o])','') = ''  then null 
                else {{ proper_br('nome_mae') }} 
            end as nome_mae,
            case 
                when regexp_replace(lower(nome_pai),r'(sem registro)|(sem informa[c|ç|ã][a|ã|][o|(oes)|(ões)])|(m[ã|a]e desconhecida)|(n[a|ã|][|o] informado)|(n[a|ã]o declarado)|(desconhecido)|(n[a|ã]o declarado)|(n[a|ã]o consta)|(sem inf[|o])','') = ''  then null 
                else {{ proper_br('nome_pai') }} 
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
                'fora de area','nao inf','plano empresa','') then null
                else {{ proper_br('nome_social') }}
            end  as nome_social,
            case 
                when lower(destino_lixo) not in ('coletado','céu aberto','outro',
                'queimado/enterrado','rede pública','sistema de esgoto (rede)',
                'filtração','sem tratamento') then null 
                else destino_lixo
            end as destino_lixo,
            safe_cast(luz_eletrica as bool) as luz_eletrica,
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
            safe_cast(em_situacao_de_rua as bool) as em_situacao_de_rua,
            case 
                when frequenta_escola = '1' then true
                when frequenta_escola = '0' then false
                else null
            end as frequenta_escola,
            split(
                regexp_replace(
                    regexp_replace(trim(meios_transporte),r'[\[|\]|"]',''),
                    r"[\']",
                    ''
                ),
                ','
            ) as meios_transporte,
            safe_cast(situacao_usuario as string) as situacao_usuario,
            split(
                regexp_replace(
                    regexp_replace(trim(doencas_condicoes),r'[\[|\]|"]',''),
                    r"[\']",
                    ''
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
                    regexp_replace(trim(meios_comunicacao),r'[\[|\]|"]',''),
                    r"[\']",
                    ''
                ),
                ','
            ) as meios_comunicacao,
            case 
                when lower(orientacao_sexual) not in ('heterossexual','homossexual (gay / lésbica)',
                'outro','bissexual') then null
                else orientacao_sexual
            end as orientacao_sexual,
            safe_cast(possui_filtro_agua as bool) as possui_filtro_agua,
            safe_cast(possui_plano_saude as bool) as possui_plano_saude,
            case 
                when lower(situacao_familiar) not in ('convive com familiar(es), sem companheira(o)',
                'vive com companheira(o) e filho(s)',
                'vive só','Convive com companheira(o) com laços conjugais e sem filhos',
                'convive com companheira(o) com filhos e/ou outro(s) familiar(es)',
                'convive com outras pessoas sem laços consanguíneos e/ou conjugais',
                'sem informações') then null
                else situacao_familiar
            end as situacao_familiar,
            safe_cast(territorio_social as bool) as territorio_social,
            case 
                when lower(abastecimento_agua) not in ('Rede Pública','Poço ou Nascente','Outro',
                'Cisterna','Carro Pipa') then null
                else abastecimento_agua
            end as abastecimento_agua,
            safe_cast(animais_no_domicilio as bool) as animais_no_domicilio,
            safe_cast(cadastro_permanente as bool) as cadastro_permanente,
            case 
                when lower(familia_localizacao) not in ('urbana','rural') then null
                else familia_localizacao
            end as familia_localizacao,
            split(
                regexp_replace(
                    regexp_replace(trim(em_caso_doenca_procura),r'[\[|\]|"]',''),
                    "[\']",
                    ''
                ),
                ','
            ) as em_caso_doenca_procura,
            -- trazer join com municipio de nascimento para termos codigos em ambos
            struct(
                nullif(municipio_nascimento,'-1') as codigo,
                nullif(municipio_nascimento,'-1') as nome
            ) as municipio_nascimento,
            struct(
                regexp_extract(municipio_residencia,r'\[IBGE: ([0-9]{1,9})\]') as codigo,
                trim(regexp_replace(municipio_residencia,'[IBGE: ([0-9]{1,9})\]','')) as nome
                --trim(regexp_extract(municipio_residencia,r'\[([A-Za-z ])IBGE: [0-9]{1,9}\]')) as nome
             ) as municipio_residencia,
            safe_cast(responsavel_familiar as bool) as responsavel_familiar,
            safe_cast(esgotamento_sanitario as string) as esgotamento_sanitario,
            safe_cast(situacao_moradia_posse as string) as situacao_moradia_posse,
            safe_cast(situacao_profissional as string) as situacao_profissional,
            safe_cast(vulnerabilidade_social as bool) as vulnerabilidade_social,
            safe_cast(familia_beneficiaria_cfc as bool) as familia_beneficiaria_cfc,
            safe_cast(data_atualizacao_cadastro as date) as data_atualizacao_cadastro,
            safe_cast(participa_grupo_comunitario as bool) as participa_grupo_comunitario,
            safe_cast(relacao_responsavel_familiar as string) as relacao_responsavel_familiar,
            safe_cast(membro_comunidade_tradicional as bool) as membro_comunidade_tradicional,
            timestamp_sub(timestamp(data_atualizacao_vinculo_equipe, "Brazil/East"),interval 2 hour) as data_atualizacao_vinculo_equipe,
            safe_cast(familia_beneficiaria_auxilio_brasil as bool) as familia_beneficiaria_auxilio_brasil,
            safe_cast(crianca_matriculada_creche_pre_escola as bool) as crianca_matriculada_creche_pre_escola,
            updated_at,
            loaded_at

        from ficha_a
    )

select *
from ficha_a_padronizada
