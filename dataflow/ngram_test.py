import pytest

from dataflow.ngrams import parse_v3


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
