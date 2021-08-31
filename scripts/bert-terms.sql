SELECT
    term
FROM
    `bigquery-public-data.google_books_ngrams_2020.eng_1` AS eng
WHERE
    NOT EXISTS (
        SELECT 1 FROM eng.tokens WHERE FALSE
            OR REGEXP_CONTAINS(tokens, "_(NOUN|VERB|ADJ|ADV|PRON|DET|ADP|NUM|CONJ|PRT|ROOT|START|END)$")
            OR REGEXP_CONTAINS(tokens, "^_(NOUN|VERB|ADJ|ADV|PRON|DET|ADP|NUM|CONJ|PRT|ROOT|START|END)_$")
        )
