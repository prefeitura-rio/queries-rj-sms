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
            safe_cast(tipo_domicilio as string) as tipo_domicilio,
            safe_cast(data_nascimento as date) as data_nascimento,
            safe_cast(pais_nascimento as string) as pais_nascimento,
            safe_cast(tipo_logradouro as string) as tipo_logradouro,
            safe_cast(tratamento_agua as string) as tratamento_agua,
            safe_cast(em_situacao_de_rua as bool) as em_situacao_de_rua,
            case 
                when frequenta_escola = '1' then true
                when frequenta_escola = '0' then false
                else null
            end as frequenta_escola,
            split(regexp_replace(meios_transporte,r'[\[|\]]',''),',') as meios_transporte,
            safe_cast(situacao_usuario as string) as situacao_usuario,
            split(regexp_replace(doencas_condicoes,r'[\[|\]]',''),',') as doencas_condicoes,
            nullif({{clean_name_string('estado_nascimento')}},'') as estado_nascimento, 
            nullif({{clean_name_string('estado_residencia')}},'') as estado_residencia,
            safe_cast(identidade_genero as string) as identidade_genero,
            split(regexp_replace(meios_comunicacao,r'[\[|\]]',''),',') as meios_comunicacao,
            safe_cast(orientacao_sexual as string) as orientacao_sexual,
            safe_cast(possui_filtro_agua as bool) as possui_filtro_agua,
            safe_cast(possui_plano_saude as bool) as possui_plano_saude,
            safe_cast(situacao_familiar as string) as situacao_familiar,
            safe_cast(territorio_social as bool) as territorio_social,
            safe_cast(abastecimento_agua as string) as abastecimento_agua,
            safe_cast(animais_no_domicilio as bool) as animais_no_domicilio,
            safe_cast(cadastro_permanente as bool) as cadastro_permanente,
            safe_cast(familia_localizacao as string) as familia_localizacao,
            split(regexp_replace(em_caso_doenca_procura,r'[\[|\]]',''),',') as em_caso_doenca_procura,
            nullif(municipio_nascimento,'-1') as municipio_nascimento,
            regexp_extract(municipio_residencia,r'\[IBGE: ([0-9]{1,9})\]') as municipio_residencia,
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
