{{
    config(
        schema="brutos_sheets",
        alias="material_mestre",
        materialized="table",
        tags=["daily"],
    )
}}

with
    source as (
        select * from {{ source("brutos_sheets_staging", "material_mestre") }}
    ),

    renamed as (
        select

            -- id
            codigo_sigma as id_material,

            -- descricao
            denominacao_generica as descricao,

            concentracao,

            forma_farmaceutica,

            apresentacao,

            outro_nome_para_busca as descricao_alternativa,

            -- hierarquia

            categoria as hierarquia_n1_categoria,
            
            subcategoria as hierarquia_n2_subcategoria,
            
            -- medicamento controlado

            e_medicamento_controlado as controlado_indicador,
            
            se_controlado_qual_tipo as controlado_tipo,
            
            -- ativo

            esta_ativo_o_item as ativo_indicador,
            
            consumo_minimo_do_item as consumo_minimo,

            -- diversos
            programa_estrategico,
            
            observacao,
            
            -- abastecimento
            
            responsavel_pelo_abastecimento as abastecimento_responsavel,
            
            frequencia_de_abastecimento as abastecimento_frequencia,
            
            medicamento_disponivel_em_gratuidade_na_farmacia_popular
            as farmacia_popular_disponibilidade_indicador,
            
            classificacao_de_importancia_xyz as classificacao_xyz,

            -- remume listagem

            e_item_remume as remume_indicador,
            
            esta_na_remume_listagem_basica as remume_listagem_basico_indicador,
            
            esta_na_remume_uso_interno_da_aps as remume_listagem_uso_interno_indicador,
            
            esta_na_remume_listagem_hospitalar as remume_listagem_hospitalar_indicador,
            
            esta_na_remume_listagem_estrategica
            as remume_listagem_estrategico_indicador,
            
            -- remume disponibilidade
            
            precisa_estar_disponivel_nos_cf_cms_cse
            as remume_disponibilidade_cf_cms_cse_indicador,
            
            precisa_estar_disponivel_nas_policlinicas
            as remume_disponibilidade_policlinica_indicador,
            
            precisa_estar_disponivel_nos_hospitais
            as remume_disponibilidade_hospital_indicador,
            
            precisa_estar_disponivel_nas_maternidades
            as remume_disponibilidade_maternidade_indicador,
            
            precisa_estar_disponivel_nos_caps as remume_disponibilidade_caps_indicador,
            
            precisa_estar_disponivel_nas_upas_e_cers
            as remume_disponibilidade_upa_cer_indicador,
            
            excecoes_preencher_os_cnes_das_unidades_onde_o_medicamento_precisa_estar_disponivel_por_alguma_particularidade_de_unidade_separar_por_ponto_e_virgula
            as remume_disponibilidade_unidades_especificas_lista,
            
            -- cadastro nos prontuarios
            
            esta_cadastrado_no_vitacare as cadastrado_sistema_vitacare_indicador,
            
            esta_no_cadastro_vitai as cadastrado_sistema_vitai_indicador,

            

        from source
    ),

    fixed as (
        select
            * except (
                id_material,
                consumo_minimo,
                farmacia_popular_disponibilidade_indicador,
                remume_disponibilidade_unidades_especificas_lista,
                remume_listagem_basico_indicador,
                remume_listagem_uso_interno_indicador,
                remume_listagem_hospitalar_indicador,
                remume_listagem_estrategico_indicador,
                remume_disponibilidade_cf_cms_cse_indicador,
                remume_disponibilidade_policlinica_indicador,
                remume_disponibilidade_hospital_indicador,
                remume_disponibilidade_maternidade_indicador,
                remume_disponibilidade_caps_indicador,
                remume_disponibilidade_upa_cer_indicador,
                cadastrado_sistema_vitacare_indicador,
                cadastrado_sistema_vitai_indicador
            ),

            -- fix numeric columns

            {{ clean_numeric_string("id_material") }} as id_material,

            coalesce(safe_cast(consumo_minimo as int64), 1) as consumo_minimo,

            -- fix array columns
            regexp_replace(
                regexp_replace(
                    remume_disponibilidade_unidades_especificas_lista,
                    r'[^0-9;]',
                    ''
                ),
                ';;+',
                ';'
            ) as remume_disponibilidade_unidades_especificas_lista,

            -- fix boolean columns

            ifnull(
                remume_listagem_basico_indicador, "nao"
            ) as remume_listagem_basico_indicador,

            ifnull(
                remume_listagem_uso_interno_indicador, "nao"
            ) as remume_listagem_uso_interno_indicador,

            ifnull(
                remume_listagem_hospitalar_indicador, "nao"
            ) as remume_listagem_hospitalar_indicador,

            ifnull(
                remume_listagem_estrategico_indicador, "nao"
            ) as remume_listagem_estrategico_indicador,

            ifnull(
                remume_disponibilidade_cf_cms_cse_indicador, "nao"
            ) as remume_disponibilidade_cf_cms_cse_indicador,

            ifnull(
                remume_disponibilidade_policlinica_indicador, "nao"
            ) as remume_disponibilidade_policlinica_indicador,

            ifnull(
                remume_disponibilidade_hospital_indicador, "nao"
            ) as remume_disponibilidade_hospital_indicador,

            ifnull(
                remume_disponibilidade_maternidade_indicador, "nao"
            ) as remume_disponibilidade_maternidade_indicador,

            ifnull(
                remume_disponibilidade_caps_indicador, "nao"
            ) as remume_disponibilidade_caps_indicador,

            ifnull(
                remume_disponibilidade_upa_cer_indicador, "nao"
            ) as remume_disponibilidade_upa_cer_indicador,

            coalesce(farmacia_popular_disponibilidade_indicador, "nao") as farmacia_popular_disponibilidade_indicador,

            if(
                contains_substr(cadastrado_sistema_vitacare_indicador, "nao"),
                "nao",
                "sim"
            ) as cadastrado_sistema_vitacare_indicador,

            if(
                contains_substr(cadastrado_sistema_vitai_indicador, "nao"),
                "nao",
                "sim"
            ) as cadastrado_sistema_vitai_indicador,

        from renamed
    ),

    array_columns_added as (
        select
            *,

            array(
                select column_name 
                from unnest([
                    struct('COMPONENTE BASICO' as column_name, remume_listagem_basico_indicador as value),
                    struct('USO INTERNO NAS UAPS' as column_name, remume_listagem_uso_interno_indicador as value),
                    struct('COMPONENTE HOSPITALAR' as column_name, remume_listagem_hospitalar_indicador as value),
                    struct('COMPONENTE ESTRATEGICO' as column_name, remume_listagem_estrategico_indicador as value)
                ])
                where value = 'sim'
            ) as remume_listagem_relacao,

            array(
                select column_name
                from (
                    select column_name, value
                    from unnest([
                        struct('CMS' as column_name, remume_disponibilidade_cf_cms_cse_indicador as value),
                        struct('CF' as column_name, remume_disponibilidade_cf_cms_cse_indicador as value), 
                        struct('CSE' as column_name, remume_disponibilidade_cf_cms_cse_indicador as value),
                        struct('POLICLINICA' as column_name, remume_disponibilidade_policlinica_indicador as value),
                        struct('HOSPITAL' as column_name, remume_disponibilidade_hospital_indicador as value),
                        struct('MATERNIDADE' as column_name, remume_disponibilidade_maternidade_indicador as value),
                        struct('CAPS' as column_name, remume_disponibilidade_caps_indicador as value),
                        struct('UPA' as column_name, remume_disponibilidade_upa_cer_indicador as value),
                        struct('CER' as column_name, remume_disponibilidade_upa_cer_indicador as value)
                    ])
                    where value = 'sim'
                    union all
                    select trim(x) as column_name, 'sim' as value
                    from unnest(split(remume_disponibilidade_unidades_especificas_lista, ';')) as x
                    where trim(x) != ''
                )
            ) as remume_disponibilidade_relacao,

        from fixed
    ),

    final as (

        select

            -- id
            id_material,

            -- descricao
            descricao,

            concentracao,

            forma_farmaceutica,

            apresentacao,

            descricao_alternativa,

            -- hierarquia

            hierarquia_n1_categoria,
            
            hierarquia_n2_subcategoria,
            
            -- medicamento controlado

            controlado_indicador,
            
            controlado_tipo,
            
            -- ativo

            ativo_indicador,
            
            consumo_minimo,

            -- diversos
            programa_estrategico,
            
            observacao,

            -- abastecimento
            
            abastecimento_responsavel,
            
            abastecimento_frequencia,
            
            farmacia_popular_disponibilidade_indicador,
            
            classificacao_xyz,

            -- remume listagem

            remume_indicador,

            remume_listagem_relacao,
            
            remume_listagem_basico_indicador,
            
            remume_listagem_uso_interno_indicador,
            
            remume_listagem_hospitalar_indicador,
            
            remume_listagem_estrategico_indicador,

            -- remume disponibilidade

            remume_disponibilidade_relacao,

            remume_disponibilidade_cf_cms_cse_indicador,
            
            remume_disponibilidade_policlinica_indicador,
            
            remume_disponibilidade_hospital_indicador,
            
            remume_disponibilidade_maternidade_indicador,
            
            remume_disponibilidade_caps_indicador,
            
            remume_disponibilidade_upa_cer_indicador,
            
            remume_disponibilidade_unidades_especificas_lista,
            
            -- cadastro nos prontuarios
            
            cadastrado_sistema_vitacare_indicador,
            
            cadastrado_sistema_vitai_indicador,

        from array_columns_added
            
    )

select *
from final