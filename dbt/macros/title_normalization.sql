{% macro normalize_game_title(expression) -%}
    nullif(
        trim(
            regexp_replace(
                regexp_replace(
                    lower(trim(cast({{ expression }} as varchar))),
                    '[^a-z0-9]+',
                    ' ',
                    'g'
                ),
                '\\s+',
                ' ',
                'g'
            )
        ),
        ''
    )
{%- endmacro %}

{% macro slugify_game_title(expression) -%}
    nullif(
        regexp_replace(
            {{ normalize_game_title(expression) }},
            ' ',
            '-',
            'g'
        ),
        ''
    )
{%- endmacro %}
