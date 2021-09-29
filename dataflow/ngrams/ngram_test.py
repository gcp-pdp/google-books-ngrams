import pytest

from ngrams import parse_v3, enrich_tag, has_tag


@pytest.mark.parametrize(
    "line,expected",
    [
        (
            "quick\t1980,1,1\t1990,2,1",
            {
                "term": "quick",
                "tokens": ["quick"],
                "term_frequency": 3,
                "document_frequency": 2,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                    {"year": 1990, "term_frequency": 2, "document_frequency": 1},
                ],
            },
        ),
        (
            "quick brown\t1980,1,1\t1990,2,1",
            {
                "term": "quick brown",
                "tokens": ["quick", "brown"],
                "term_frequency": 3,
                "document_frequency": 2,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                    {"year": 1990, "term_frequency": 2, "document_frequency": 1},
                ],
            },
        ),
        (
            "quick brown fox\t1980,1,1\t1990,2,1",
            {
                "term": "quick brown fox",
                "tokens": ["quick", "brown", "fox"],
                "term_frequency": 3,
                "document_frequency": 2,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                    {"year": 1990, "term_frequency": 2, "document_frequency": 1},
                ],
            },
        ),
        (
            "quick brown fox jumps\t1980,1,1\t1990,2,1",
            {
                "term": "quick brown fox jumps",
                "tokens": ["quick", "brown", "fox", "jumps"],
                "term_frequency": 3,
                "document_frequency": 2,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                    {"year": 1990, "term_frequency": 2, "document_frequency": 1},
                ],
            },
        ),
        (
            "quick brown fox jumps over\t1980,1,1\t1990,2,1",
            {
                "term": "quick brown fox jumps over",
                "tokens": ["quick", "brown", "fox", "jumps", "over"],
                "term_frequency": 3,
                "document_frequency": 2,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                    {"year": 1990, "term_frequency": 2, "document_frequency": 1},
                ],
            },
        ),
    ],
)
def test_parse_v3(line, expected):
    result = parse_v3(line)
    assert result == expected


@pytest.mark.parametrize(
    "element,expected",
    [
        (
            {
                "term": "A_NOUN",
                "tokens": ["A_NOUN"],
                "term_frequency": 1,
                "document_frequency": 1,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                ],
            },
            {
                "term": "A_NOUN",
                "tokens": ["A_NOUN"],
                "term_frequency": 1,
                "document_frequency": 1,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                ],
                "has_tag": True,
            },
        ),
        (
            {
                "term": "COVID19",
                "tokens": ["COVID19"],
                "term_frequency": 1,
                "document_frequency": 1,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                ],
            },
            {
                "term": "COVID19",
                "tokens": ["COVID19"],
                "term_frequency": 1,
                "document_frequency": 1,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                ],
                "has_tag": False,
            },
        ),
        (
            {
                "term": "DO NOT _START_ NOW",
                "tokens": ["DO", "NOT", "_START_", "NOW"],
                "term_frequency": 1,
                "document_frequency": 1,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                ],
            },
            {
                "term": "DO NOT _START_ NOW",
                "tokens": ["DO", "NOT", "_START_", "NOW"],
                "term_frequency": 1,
                "document_frequency": 1,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                ],
                "has_tag": True,
            },
        ),
        (
            {
                "term": "DO NOT START NOW",
                "tokens": ["DO", "NOT", "START", "NOW"],
                "term_frequency": 1,
                "document_frequency": 1,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                ],
            },
            {
                "term": "DO NOT START NOW",
                "tokens": ["DO", "NOT", "START", "NOW"],
                "term_frequency": 1,
                "document_frequency": 1,
                "years": [
                    {"year": 1980, "term_frequency": 1, "document_frequency": 1},
                ],
                "has_tag": False,
            },
        ),
    ],
)
def test_enrich_tag(element, expected):
    result = enrich_tag(element)
    assert result == expected


@pytest.mark.parametrize(
    "tokens,expected",
    [
        (["A_NOUN"], True),
        (["A_VERB"], True),
        (["A_ADJ"], True),
        (["A_ADV"], True),
        (["A_PRON"], True),
        (["A_DET"], True),
        (["A_ADP"], True),
        (["A_NUM"], True),
        (["A_CONJ"], True),
        (["A_PRT"], True),
        (["A_ROOT"], True),
        (["A_START"], True),
        (["A_END"], True),
        (["_NOUN_"], True),
        (["_VERB_"], True),
        (["_ADJ_"], True),
        (["_ADV_"], True),
        (["_PRON_"], True),
        (["_DET_"], True),
        (["_ADP_"], True),
        (["_NUM_"], True),
        (["_CONJ_"], True),
        (["_PRT_"], True),
        (["_ROOT_"], True),
        (["_START_"], True),
        (["_END_"], True),
        (["_NOUN", "This"], True),
        (["_VERB", "This"], True),
        (["_ADJ", "This"], True),
        (["_ADV", "This"], True),
        (["_PRON", "This"], True),
        (["_DET", "This"], True),
        (["_ADP", "This"], True),
        (["_NUM", "This"], True),
        (["_CONJ", "This"], True),
        (["_PRT", "This"], True),
        (["_ROOT", "This"], True),
        (["_START", "This"], True),
        (["_END", "This"], True),
        (["_NOUN_", "This"], True),
        (["_VERB_", "This"], True),
        (["_ADJ_", "This"], True),
        (["_ADV_", "This"], True),
        (["_PRON_", "This"], True),
        (["_DET_", "This"], True),
        (["_ADP_", "This"], True),
        (["_NUM_", "This"], True),
        (["_CONJ_", "This"], True),
        (["_PRT_", "This"], True),
        (["_ROOT_", "This"], True),
        (["_START_", "This"], True),
        (["_END_", "This"], True),
        (["Quick", "brown", "fox"], False),
        (["NOUN_", "_PROD_", "_PROD"], False),
        (["1234", "1,200.00"], False),
        (["!", "_", "-*+%", ";"], False),
    ],
)
def test_has_tag(tokens, expected):
    result = has_tag(tokens)
    assert result == expected
