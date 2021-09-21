import argparse
import json
import logging
import os

import apache_beam as beam
import tensorflow as tf
import tensorflow_hub as hub
import tensorflow_text as text
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions


def read_bigquery_schema_from_file(filepath):
    with open(filepath) as file_handle:
        content = file_handle.read()
        return json.loads(content)


class Preprocess(beam.DoFn):
    def __init__(self):
        pass

    def setup(self):
        self.preprocessor = hub.load(
            "https://tfhub.dev/tensorflow/bert_en_uncased_preprocess/2"
        )

    def process(self, elements, *args, **kwargs):
        encoder_inputs = self.preprocessor(tf.convert_to_tensor(elements))
        yield [
            {
                "preprocessed_term": element,
                "input_type_ids": input_type_ids,
                "input_word_ids": input_word_ids,
                "input_mask": input_mask,
            }
            for (element, input_type_ids, input_word_ids, input_mask) in zip(
                elements,
                encoder_inputs["input_type_ids"],
                encoder_inputs["input_word_ids"],
                encoder_inputs["input_mask"],
            )
        ]


class Encode(beam.DoFn):
    def __init__(self):
        pass

    def setup(self):
        self.encoder = hub.KerasLayer(
            "https://tfhub.dev/tensorflow/small_bert/bert_en_uncased_L-2_H-128_A-2/2",
            trainable=True,
        )

    def process(self, elements, *args, **kwargs):
        encoder_inputs = {
            "input_type_ids": [e["input_type_ids"] for e in elements],
            "input_word_ids": [e["input_word_ids"] for e in elements],
            "input_mask": [e["input_mask"] for e in elements],
        }
        encoder_outputs = self.encoder(encoder_inputs)
        yield [
            {
                "preprocessed_term": element["preprocessed_term"],
                "embeddings": encoder_output.numpy().tolist(),
            }
            for (element, encoder_output) in zip(elements, encoder_outputs["default"])
        ]


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
        default="gcp-pdp-words-dev:words_dev.eng_bert_embeddings",
        required=True,
        help="The BigQuery output table to write to.",
    )
    parser.add_argument(
        "--batch-size",
        default=1000,
        type=int,
        required=True,
        help="The number of batch records to be processed in Tensorflow.",
    )
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(beam_args)
    beam_options.view_as(SetupOptions).save_main_session = True

    dir_path = os.path.dirname(os.path.realpath(__file__))
    schema_path = os.path.join(dir_path, "schema.json")
    table_schema = {"fields": read_bigquery_schema_from_file(schema_path)}

    with beam.Pipeline(options=beam_options) as pipeline:
        embeddings = (
            pipeline
            | "Read BigQuery"
            >> beam.io.ReadFromBigQuery(
                query=f"SELECT DISTINCT preprocessed_term FROM `{args.input_table}`",
                use_standard_sql=True,
                flatten_results=False,
            )
            | "Extract term" >> beam.Map(lambda element: element["preprocessed_term"])
            | "Batch elements"
            >> beam.BatchElements(
                min_batch_size=args.batch_size, max_batch_size=args.batch_size
            )
            | "Preprocess" >> beam.ParDo(Preprocess())
            | "Encode" >> beam.ParDo(Encode())
            | "Flatten" >> beam.FlatMap(lambda elements: elements)
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
