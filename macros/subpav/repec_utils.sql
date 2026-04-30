{% macro repec_origem_unidade_para_cnes(campo) %}
    case
        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_JOSE_BREVES_DOS_SANTOS'
        ) then '2269902'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_MADRE_TERESA'
        ) then '2273640'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_NECKER_PINTO'
        ) then '2280779'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_MARIA_CRISTINA_ROMA_PAUGARTTEN'
        ) then '2295032'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_NAGIB_JORGE_FARAH'
        ) then '2296535'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_AMERICO_VELOSO'
        ) then '2296551'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_JOAO_CANDIDO'
        ) then '3784959'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_ZILDA_ARNS'
        ) then '3784975'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_ALOYSIO_AUGUSTO_NOVIS'
        ) then '5179726'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CSE_GERMANO_SINVAL_FARIA|CAP31_CSE_GERMANO_SINVAL_FARIA'
        ) then '5456932'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_IRACI_LOPES'
        ) then '5457009'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_PARQUE_ROYAL'
        ) then '5467136'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_ADIB_JATENE'
        ) then '5476607'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_VILA_DO_JOAO'
        ) then '5476844'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_AUGUSTO_BOAL'
        ) then '6023320'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_VICTOR_VALLA'
        ) then '6514022'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_RODRIGO_Y_AGUILAR_ROIG'
        ) then '6524486'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_MARIA_SEBASTIANA(_DE)?_OLIVEIRA'
        ) then '6568491'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_HEITOR_DOS_PRAZERES'
        ) then '6664040'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_FELIPPE_CARDOSO'
        ) then '6664075'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_SAO_GODOFREDO'
        ) then '6664164'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_ASSIS_VALENTE'
        ) then '6804209'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_JOAOSINHO_TRINTA'
        ) then '6932916'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_NEWTON_ALVES_CARDOZO'
        ) then '7856954'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_EIDIMIR_THIAGO(_DE)?_SOUZA'
        ) then '7985657'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_NILDA_CAMPOS_DE_LIMA'
        ) then '9016805'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_WILMA_COSTA'
        ) then '9072659'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_KLEBEL(_DE)?_OLIVEIRA_ROCHA'
        ) then '9075143'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_VALTER_FELISBINO(_DE)?_SOUZA'
        ) then '9107835'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_DINIZ_BATISTA_DOS_SANTOS'
        ) then '9345515'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CMS_JOSE_PARANHOS_FONTENELLE'
        ) then '9391983'

        when regexp_contains(
            upper(trim({{ remove_decode_chars_error("cast(" ~ campo ~ " as string)") }})),
            r'CAP31_CF_JEREMIAS_MORAES_DA_SILVA'
        ) then '9442251'

        else null
    end
{% endmacro %}