{{
    config(
        alias="estabelecimento",
        schema= "brutos_cnes_ftp"
    )
}}

with
    source as (select * from {{ source("br_ms_cnes", "estabelecimento") }}),
    renamed as (
        select
            {{ adapter.quote("ano") }},
            {{ adapter.quote("mes") }},
            {{ adapter.quote("sigla_uf") }},
            {{ adapter.quote("ano_atualizacao") }},
            {{ adapter.quote("mes_atualizacao") }},
            {{ adapter.quote("id_municipio") }},
            {{ adapter.quote("id_municipio_6") }},
            {{ adapter.quote("id_regiao_saude") }},
            {{ adapter.quote("id_microrregiao_saude") }},
            {{ adapter.quote("id_distrito_sanitario") }},
            {{ adapter.quote("id_distrito_administrativo") }},
            {{ adapter.quote("cep") }},
            {{ adapter.quote("id_estabelecimento_cnes") }},
            {{ adapter.quote("tipo_pessoa") }},
            {{ adapter.quote("cpf_cnpj") }},
            {{ adapter.quote("tipo_grau_dependencia") }},
            {{ adapter.quote("cnpj_mantenedora") }},
            {{ adapter.quote("tipo_retencao_tributos_mantenedora") }},
            {{ adapter.quote("indicador_vinculo_sus") }},
            {{ adapter.quote("tipo_gestao") }},
            {{ adapter.quote("tipo_esfera_administrativa") }},
            {{ adapter.quote("tipo_retencao_tributos") }},
            {{ adapter.quote("tipo_atividade_ensino_pesquisa") }},
            {{ adapter.quote("tipo_natureza_administrativa") }},
            {{ adapter.quote("id_natureza_juridica") }},
            {{ adapter.quote("tipo_fluxo_atendimento") }},
            {{ adapter.quote("tipo_unidade") }},
            {{ adapter.quote("tipo_turno") }},
            {{ adapter.quote("tipo_nivel_hierarquia") }},
            {{ adapter.quote("tipo_prestador") }},
            {{ adapter.quote("banco") }},
            {{ adapter.quote("agencia") }},
            {{ adapter.quote("conta_corrente") }},
            {{ adapter.quote("id_contrato_municipio_sus") }},
            {{ adapter.quote("data_publicacao_contrato_municipal") }},
            {{ adapter.quote("data_publicacao_contrato_estadual") }},
            {{ adapter.quote("id_contrato_estado_sus") }},
            {{ adapter.quote("numero_alvara") }},
            {{ adapter.quote("data_expedicao_alvara") }},
            {{ adapter.quote("tipo_orgao_expedidor") }},
            {{ adapter.quote("tipo_avaliacao_acreditacao_hospitalar") }},
            {{ adapter.quote("tipo_classificacao_acreditacao_hospitalar") }},
            {{ adapter.quote("ano_acreditacao") }},
            {{ adapter.quote("mes_acreditacao") }},
            {{ adapter.quote("tipo_avaliacao_pnass") }},
            {{ adapter.quote("ano_avaliacao_pnass") }},
            {{ adapter.quote("mes_avaliacao_pnass") }},
            {{ adapter.quote("indicador_atencao_ambulatorial") }},
            {{ adapter.quote("indicador_gestao_basica_ambulatorial_estadual") }},
            {{ adapter.quote("indicador_gestao_basica_ambulatorial_municipal") }},
            {{ adapter.quote("indicador_gestao_media_ambulatorial_estadual") }},
            {{ adapter.quote("indicador_gestao_media_ambulatorial_municipal") }},
            {{ adapter.quote("indicador_gestao_alta_ambulatorial_estadual") }},
            {{ adapter.quote("indicador_gestao_alta_ambulatorial_municipal") }},
            {{ adapter.quote("indicador_atencao_hospitalar") }},
            {{ adapter.quote("indicador_gestao_media_hospitalar_estadual") }},
            {{ adapter.quote("indicador_gestao_media_hospitalar_municipal") }},
            {{ adapter.quote("indicador_gestao_alta_hospitalar_estadual") }},
            {{ adapter.quote("indicador_gestao_alta_hospitalar_municipal") }},
            {{ adapter.quote("indicador_gestao_hospitalar_estadual") }},
            {{ adapter.quote("indicador_gestao_hospitalar_municipal") }},
            {{ adapter.quote("indicador_leito_hospitalar") }},
            {{ adapter.quote("quantidade_leito_cirurgico") }},
            {{ adapter.quote("quantidade_leito_clinico") }},
            {{ adapter.quote("quantidade_leito_complementar") }},
            {{ adapter.quote("quantidade_leito_repouso_pediatrico_urgencia") }},
            {{ adapter.quote("quantidade_leito_repouso_feminino_urgencia") }},
            {{ adapter.quote("quantidade_leito_repouso_masculino_urgencia") }},
            {{ adapter.quote("quantidade_leito_repouso_indiferenciado_urgencia") }},
            {{ adapter.quote("indicador_instalacao_urgencia") }},
            {{ adapter.quote("quantidade_consultorio_pediatrico_urgencia") }},
            {{ adapter.quote("quantidade_consultorio_feminino_urgencia") }},
            {{ adapter.quote("quantidade_consultorio_masculino_urgencia") }},
            {{ adapter.quote("quantidade_consultorio_indiferenciado_urgencia") }},
            {{ adapter.quote("quantidade_consultorio_odontologia_urgencia") }},
            {{ adapter.quote("quantidade_sala_repouso_pediatrico_urgencia") }},
            {{ adapter.quote("quantidade_sala_repouso_feminino_urgencia") }},
            {{ adapter.quote("quantidade_sala_repouso_masculino_urgencia") }},
            {{ adapter.quote("quantidade_sala_repouso_indiferenciado_urgencia") }},
            {{ adapter.quote("quantidade_equipos_odontologia_urgencia") }},
            {{ adapter.quote("quantidade_sala_higienizacao_urgencia") }},
            {{ adapter.quote("quantidade_sala_gesso_urgencia") }},
            {{ adapter.quote("quantidade_sala_curativo_urgencia") }},
            {{ adapter.quote("quantidade_sala_pequena_cirurgia_urgencia") }},
            {{ adapter.quote("quantidade_consultorio_medico_urgencia") }},
            {{ adapter.quote("indicador_instalacao_ambulatorial") }},
            {{ adapter.quote("quantidade_consultorio_clinica_basica_ambulatorial") }},
            {{
                adapter.quote(
                    "quantidade_consultorio_clinica_especializada_ambulatorial"
                )
            }},
            {{
                adapter.quote(
                    "quantidade_consultorio_clinica_indiferenciada_ambulatorial"
                )
            }},
            {{ adapter.quote("quantidade_consultorio_nao_medico_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_repouso_feminino_ambulatorial") }},
            {{ adapter.quote("quantidade_leito_repouso_feminino_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_repouso_masculino_ambulatorial") }},
            {{ adapter.quote("quantidade_leito_repouso_masculino_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_repouso_pediatrico_ambulatorial") }},
            {{ adapter.quote("quantidade_leito_repouso_pediatrico_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_repouso_indiferenciado_ambulatorial") }},
            {{ adapter.quote("quantidade_leito_repouso_indiferenciado_ambulatorial") }},
            {{ adapter.quote("quantidade_consultorio_odontologia_ambulatorial") }},
            {{ adapter.quote("quantidade_equipos_odontologia_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_pequena_cirurgia_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_enfermagem_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_imunizacao_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_nebulizacao_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_gesso_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_curativo_ambulatorial") }},
            {{ adapter.quote("quantidade_sala_cirurgia_ambulatorial") }},
            {{ adapter.quote("indicador_instalacao_hospitalar") }},
            {{ adapter.quote("indicador_instalacao_hospitalar_centro_cirurgico") }},
            {{ adapter.quote("quantidade_sala_cirurgia_centro_cirurgico") }},
            {{ adapter.quote("quantidade_sala_recuperacao_centro_cirurgico") }},
            {{ adapter.quote("quantidade_leito_recuperacao_centro_cirurgico") }},
            {{
                adapter.quote(
                    "quantidade_sala_cirurgia_ambulatorial_centro_cirurgico"
                )
            }},
            {{ adapter.quote("indicador_instalacao_hospitalar_centro_obstetrico") }},
            {{ adapter.quote("quantidade_sala_pre_parto_centro_obstetrico") }},
            {{ adapter.quote("quantidade_leito_pre_parto_centro_obstetrico") }},
            {{ adapter.quote("quantidade_sala_parto_normal_centro_obstetrico") }},
            {{ adapter.quote("quantidade_sala_curetagem_centro_obstetrico") }},
            {{ adapter.quote("quantidade_sala_cirurgia_centro_obstetrico") }},
            {{ adapter.quote("indicador_instalacao_hospitalar_neonatal") }},
            {{ adapter.quote("quantidade_leito_recem_nascido_normal_neonatal") }},
            {{ adapter.quote("quantidade_leito_recem_nascido_patologico_neonatal") }},
            {{ adapter.quote("quantidade_leito_conjunto_neonatal") }},
            {{ adapter.quote("indicador_servico_apoio") }},
            {{ adapter.quote("indicador_servico_same_spp_proprio") }},
            {{ adapter.quote("indicador_servico_same_spp_terceirizado") }},
            {{ adapter.quote("indicador_servico_social_proprio") }},
            {{ adapter.quote("indicador_servico_social_terceirizado") }},
            {{ adapter.quote("indicador_servico_farmacia_proprio") }},
            {{ adapter.quote("indicador_servico_farmacia_terceirizado") }},
            {{ adapter.quote("indicador_servico_esterilizacao_proprio") }},
            {{ adapter.quote("indicador_servico_esterilizacao_terceirizado") }},
            {{ adapter.quote("indicador_servico_nutricao_proprio") }},
            {{ adapter.quote("indicador_servico_nutricao_terceirizado") }},
            {{ adapter.quote("indicador_servico_lactario_proprio") }},
            {{ adapter.quote("indicador_servico_lactario_terceirizado") }},
            {{ adapter.quote("indicador_servico_banco_leite_proprio") }},
            {{ adapter.quote("indicador_servico_banco_leite_terceirizado") }},
            {{ adapter.quote("indicador_servico_lavanderia_proprio") }},
            {{ adapter.quote("indicador_servico_lavanderia_terceirizado") }},
            {{ adapter.quote("indicador_servico_manutencao_proprio") }},
            {{ adapter.quote("indicador_servico_manutencao_terceirizado") }},
            {{ adapter.quote("indicador_servico_ambulancia_proprio") }},
            {{ adapter.quote("indicador_servico_ambulancia_terceirizado") }},
            {{ adapter.quote("indicador_servico_necroterio_proprio") }},
            {{ adapter.quote("indicador_servico_necroterio_terceirizado") }},
            {{ adapter.quote("indicador_coleta_residuo") }},
            {{ adapter.quote("indicador_coleta_residuo_biologico") }},
            {{ adapter.quote("indicador_coleta_residuo_quimico") }},
            {{ adapter.quote("indicador_coleta_rejeito_radioativo") }},
            {{ adapter.quote("indicador_coleta_rejeito_comum") }},
            {{ adapter.quote("indicador_comissao") }},
            {{ adapter.quote("indicador_comissao_etica_medica") }},
            {{ adapter.quote("indicador_comissao_etica_enfermagem") }},
            {{ adapter.quote("indicador_comissao_farmacia_terapeutica") }},
            {{ adapter.quote("indicador_comissao_controle_infeccao") }},
            {{ adapter.quote("indicador_comissao_apropriacao_custos") }},
            {{ adapter.quote("indicador_comissao_cipa") }},
            {{ adapter.quote("indicador_comissao_revisao_prontuario") }},
            {{ adapter.quote("indicador_comissao_revisao_documentacao") }},
            {{ adapter.quote("indicador_comissao_analise_obito_biopisias") }},
            {{ adapter.quote("indicador_comissao_investigacao_epidemiologica") }},
            {{ adapter.quote("indicador_comissao_notificacao_doencas") }},
            {{ adapter.quote("indicador_comissao_zoonose_vetores") }},
            {{ adapter.quote("indicador_atendimento_prestado") }},
            {{ adapter.quote("indicador_atendimento_internacao_sus") }},
            {{ adapter.quote("indicador_atendimento_internacao_particular") }},
            {{ adapter.quote("indicador_atendimento_internacao_plano_seguro_proprio") }},
            {{
                adapter.quote(
                    "indicador_atendimento_internacao_plano_seguro_terceiro"
                )
            }},
            {{ adapter.quote("indicador_atendimento_internacao_plano_saude_publico") }},
            {{ adapter.quote("indicador_atendimento_internacao_plano_saude_privado") }},
            {{ adapter.quote("indicador_atendimento_ambulatorial_sus") }},
            {{ adapter.quote("indicador_atendimento_ambulatorial_particular") }},
            {{
                adapter.quote(
                    "indicador_atendimento_ambulatorial_plano_seguro_proprio"
                )
            }},
            {{
                adapter.quote(
                    "indicador_atendimento_ambulatorial_plano_seguro_terceiro"
                )
            }},
            {{
                adapter.quote(
                    "indicador_atendimento_ambulatorial_plano_saude_publico"
                )
            }},
            {{
                adapter.quote(
                    "indicador_atendimento_ambulatorial_plano_saude_privado"
                )
            }},
            {{ adapter.quote("indicador_atendimento_sadt_sus") }},
            {{ adapter.quote("indicador_atendimento_sadt_privado") }},
            {{ adapter.quote("indicador_atendimento_sadt_plano_seguro_proprio") }},
            {{ adapter.quote("indicador_atendimento_sadt_plano_seguro_terceiro") }},
            {{ adapter.quote("indicador_atendimento_sadt_plano_saude_publico") }},
            {{ adapter.quote("indicador_atendimento_sadt_plano_saude_privado") }},
            {{ adapter.quote("indicador_atendimento_urgencia_sus") }},
            {{ adapter.quote("indicador_atendimento_urgencia_privado") }},
            {{ adapter.quote("indicador_atendimento_urgencia_plano_seguro_proprio") }},
            {{ adapter.quote("indicador_atendimento_urgencia_plano_seguro_terceiro") }},
            {{ adapter.quote("indicador_atendimento_urgencia_plano_saude_publico") }},
            {{ adapter.quote("indicador_atendimento_urgencia_plano_saude_privado") }},
            {{ adapter.quote("indicador_atendimento_outros_sus") }},
            {{ adapter.quote("indicador_atendimento_outros_privado") }},
            {{ adapter.quote("indicador_atendimento_outros_plano_seguro_proprio") }},
            {{ adapter.quote("indicador_atendimento_outros_plano_seguro_terceiro") }},
            {{ adapter.quote("indicador_atendimento_outros_plano_saude_publico") }},
            {{ adapter.quote("indicador_atendimento_outros_plano_saude_privado") }},
            {{ adapter.quote("indicador_atendimento_vigilancia_sus") }},
            {{ adapter.quote("indicador_atendimento_vigilancia_privado") }},
            {{ adapter.quote("indicador_atendimento_vigilancia_plano_seguro_proprio") }},
            {{
                adapter.quote(
                    "indicador_atendimento_vigilancia_plano_seguro_terceiro"
                )
            }},
            {{ adapter.quote("indicador_atendimento_vigilancia_plano_saude_publico") }},
            {{ adapter.quote("indicador_atendimento_vigilancia_plano_saude_privado") }},
            {{ adapter.quote("indicador_atendimento_regulacao_sus") }},
            {{ adapter.quote("indicador_atendimento_regulacao_privado") }},
            {{ adapter.quote("indicador_atendimento_regulacao_plano_seguro_proprio") }},
            {{ adapter.quote("indicador_atendimento_regulacao_plano_seguro_terceiro") }},
            {{ adapter.quote("indicador_atendimento_regulacao_plano_saude_publico") }},
            {{ adapter.quote("indicador_atendimento_regulacao_plano_saude_privado") }}

        from source
    )
select *
from renamed
