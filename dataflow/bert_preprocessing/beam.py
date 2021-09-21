import argparse
import json
import logging
import os
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def read_bigquery_schema_from_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return json.loads(content)


class Sanitize(beam.DoFn):
    def __init__(
        self, filters='!"#$%&()*+,-./:;<=>?@[\\]^_`{|}~\t\n', replace="", lower=True
    ):
        self.translate_map = str.maketrans({c: replace for c in filters})
        self.lower = lower

    def sanitize(self, text):
        if self.lower:
            text = text.lower()
        translated_text = text.translate(self.translate_map)
        return re.sub("[ ]+", " ", translated_text.strip())

    def process(self, element, *args, **kwargs):
        preprocessed_term = self.sanitize(element["term"])
        yield {
            "term": element["term"],
            "preprocessed_term": preprocessed_term,
            "token_count": len(element["tokens"]),
        }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-table",
        default="gcp-pdp-words-dev:words_dev.eng_1",
        required=True,
        help="The BigQuery table to read ngram.",
    )
    parser.add_argument(
        "--output-table",
        default="gcp-pdp-words-dev:words_dev.eng_bert_preprocessed",
        required=True,
        help="The BigQuery preprocessed table to write to.",
    )
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(beam_args)
    beam_options.view_as(SetupOptions).save_main_session = True

    dir_path = os.path.dirname(os.path.realpath(__file__))
    schema_path = os.path.join(dir_path, "schema.json")
    table_schema = {"fields": read_bigquery_schema_from_file(schema_path)}
    bq_parameters = {
        "rangePartitioning": {
            "field": "token_count",
            "range": {"start": 1, "end": 6, "interval": 1},
        }
    }

    read_query = " UNION ALL ".join(
        [
            f"SELECT term, tokens FROM `{table}` WHERE has_tag=false"
            for table in args.input_table.split(",")
        ]
    )

    with beam.Pipeline(options=beam_options) as pipeline:
        preprocessed_terms = (
            pipeline
            | "Read BigQuery"
            >> beam.io.ReadFromBigQuery(
                query=read_query,
                use_standard_sql=True,
                flatten_results=False,
            )
            | "Sanitize" >> beam.ParDo(Sanitize())
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                additional_bq_parameters=bq_parameters,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
