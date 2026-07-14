{% macro padronizar_dose(coluna_dose) %}
    CASE 
        -- Tratamento de Nulos e Vazios
        WHEN {{ coluna_dose }} IS NULL OR TRIM({{ coluna_dose }}) = '' THEN NULL

        -- Padronização para Caixa Alta/Tratamento básico de espaços
        WHEN LOWER(TRIM({{ coluna_dose }})) IN (
            '1ª dose', '1 dose', '1st dose', '1ª dose dobrada', 
            '1ª dose revacinação', '1ª dose revacinação dobrada', '1ª dose fracionada'
        ) THEN '1ª Dose'

        WHEN LOWER(TRIM({{ coluna_dose }})) IN (
            '2ª dose', '2 dose', '2nd dose', '2ª dose dobrada', 
            '2ª dose revacinação', '2ª dose revacinação dobrada', '2ª dose fracionada'
        ) THEN '2ª Dose'

        WHEN LOWER(TRIM({{ coluna_dose }})) IN (
            '3ª dose', '3 dose', '3rd dose', '3ª dose dobrada', 
            '3ª dose revacinação', '3ª dose revacinação dobrada', '3ª dose fracionada'
        ) THEN '3ª Dose'

        WHEN LOWER(TRIM({{ coluna_dose }})) IN (
            '4ª dose', '4 dose', '4th dose', '4ª dose dobrada', 
            '4ª dose revacinação', '4ª dose revacinação dobrada'
        ) THEN '4ª Dose'

        WHEN LOWER(TRIM({{ coluna_dose }})) IN (
            '5ª dose', '5 dose', '5th dose', '5ª dose revacinação'
        ) THEN '5ª Dose'

        -- Reforços
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('1º reforço', '1 reforço', '1 reforo', '1st booster') THEN '1º Reforço'
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('2º reforço', '2 reforço', '2 reforo', '2nd booster') THEN '2º Reforço'
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('3º reforço', '3 reforço', '3 reforo', '3rd booster') THEN '3º Reforço'
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('4º reforço', '4º reforço') THEN '4º Reforço'
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('5º reforço') THEN '5º Reforço'
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('6º reforço') THEN '6º Reforço'
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('reforço', 'reforo', 'booster') THEN 'Reforço'

        -- Dose Única
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('única', 'dose única', 'dose nica', 'single dose', 'dose d') THEN 'Dose Única'

        -- Dose Adicional / Inicial / Geral
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('dose adicional', 'dose adicional', 'aditional dose') THEN 'Dose Adicional'
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('dose inicial', 'initial dose') THEN 'Dose Inicial'
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('dose') THEN 'Dose'

        -- Revacinação
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('revacinação', 'revacinao', 're-vaccination') THEN 'Revacinação'

        -- Tratamentos específicos (Sipni)
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com uma dose' THEN 'Tratamento com 1 dose'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com duas doses' THEN 'Tratamento com 2 doses'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com três doses' THEN 'Tratamento com 3 doses'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com quatro doses' THEN 'Tratamento com 4 doses'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com cinco doses' THEN 'Tratamento com 5 doses'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com oito doses' THEN 'Tratamento com 8 doses'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com nove doses' THEN 'Tratamento com 9 doses'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com treze doses' THEN 'Tratamento com 13 doses'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com quartorze doses' THEN 'Tratamento com 14 doses'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'tratamento com vinte doses' THEN 'Tratamento com 20 doses'
        WHEN LOWER(TRIM({{ coluna_dose }})) = 'dose zero' THEN 'Dose Zero'

        -- Profilaxias (Sipni)
        WHEN LOWER(TRIM({{ coluna_dose }})) LIKE 'profilaxia com%' THEN 
            REGEXP_REPLACE(
                REGEXP_REPLACE(LOWER(TRIM({{ coluna_dose }})), 'frascos-ampolas/ampolas|frasco-ampola/ampola', 'ampola(s)'),
                'profilaxia com ', 'Profilaxia: '
            )

        -- Outros / Não identificados
        WHEN LOWER(TRIM({{ coluna_dose }})) IN ('outro', 'outra', 'other') THEN 'Outro'

        ELSE INITCAP({{ coluna_dose }}) -- Mantém o formato original capitalizado caso surja algo novo
    END
{% endmacro %}