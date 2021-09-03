UPDATE `{{ project_id }}.{{ dataset }}.{{ table }}` AS words
SET has_tag=EXISTS(select 1 FROM UNNEST(words.tokens) AS token
    WHERE
    REGEXP_CONTAINS(token, "_(NOUN|VERB|ADJ|ADV|PRON|DET|ADP|NUM|CONJ|PRT|ROOT|START|END)$")
    OR REGEXP_CONTAINS(token, "^_(NOUN|VERB|ADJ|ADV|PRON|DET|ADP|NUM|CONJ|PRT|ROOT|START|END)_$"))
WHERE true