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


def parse_v3(line):
    values = line.split("\t")
    word, *year_list = values
    years = []
    sum_term_freq = 0
    sum_doc_freq = 0
    for value in year_list:
        year, term_freq, doc_freq = value.split(",", 3)
        sum_term_freq += int(term_freq)
        sum_doc_freq += int(doc_freq)
        years.append(
            {
                "year": int(year),
                "term_frequency": int(term_freq),
                "document_frequency": int(doc_freq),
            }
        )
    return {
        "term": word,
        "tokens": word.split(" "),
        "term_frequency": sum_term_freq,
        "document_frequency": sum_doc_freq,
        "years": years,
    }


def enrich_tag(element):
    return {**element, "has_tag": has_tag(element["tokens"])}


def has_tag(tokens):
    for token in tokens:
        if re.search(
            "_(NOUN|VERB|ADJ|ADV|PRON|DET|ADP|NUM|CONJ|PRT|ROOT|START|END)$", token
        ) is not None or re.search(
            "^_(NOUN|VERB|ADJ|ADV|PRON|DET|ADP|NUM|CONJ|PRT|ROOT|START|END)_$", token
        ):
            return True
    return False


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-file",
        default="gs://books/ngrams/books/20200217/eng/1-*.gz",
        required=True,
        help="The file path for the input text to process.",
    )
    parser.add_argument(
        "--output-table", required=True, help="The BigQuery table to write."
    )
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(beam_args)
    beam_options.view_as(SetupOptions).save_main_session = True

    dir_path = os.path.dirname(os.path.realpath(__file__))
    schema_path = os.path.join(dir_path, "ngram_schema.json")
    table_schema = {"fields": read_bigquery_schema_from_file(schema_path)}

    with beam.Pipeline(options=beam_options) as pipeline:
        lines = (
            pipeline
            | "Read files" >> beam.io.ReadFromText(args.input_file)
            | "Map to BigQuery rows" >> beam.Map(parse_v3)
            | "Enrich tag" >> beam.Map(enrich_tag)
            | "Write to BigQuery"
            >> beam.io.WriteToBigQuery(
                args.output_table,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.WARNING)
    run()
