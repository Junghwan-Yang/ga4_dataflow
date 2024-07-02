import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
import json
import os


class ChosunGABigQueryOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument('--table_spec', required=True, help='The BQ table spec of Chosun GA Data')
        parser.add_argument('--output', required=True, help='The outputPath of result')
        parser.add_argument('--custom_gcs_temp_location', required=True,
                            help='The Custom GCS Temp Location for write to BQ')
        parser.add_argument('--failed_output', required=True, help='The outputPath of failed rows')


class ChosunGADataProcessing(beam.DoFn):
    def process(self, element):
        def extract_key_value_params(row, property):
            if property in row and isinstance(row[property], list):
                return [{item['key']: item['value'] for item in row[property]}]
            return None

        event_params = extract_key_value_params(element, 'event_params')
        if event_params:
            del element['event_params']

        # Convert specific fields to long
        long_fields = [
            "event_timestamp",
            "event_previous_timestamp",
            "user_first_touch_timestamp",
            "event_bundle_sequence_id",
            "event_server_timestamp_offset",
            "time_zone_offset_seconds",
        ]
        for field in long_fields:
            if field in element:
                element[field] = int(element[field])

        if 'device' in element and isinstance(element['device'], dict):
            if 'time_zone_offset_seconds' in element['device']:
                element['device']['time_zone_offset_seconds'] = int(element['device']['time_zone_offset_seconds'])

        event_row = {}
        if event_params:
            for param in event_params:
                event_row.update(param)
        element['event_params'] = event_row

        user_properties_params = extract_key_value_params(element, 'user_properties')
        if user_properties_params:
            del element['user_properties']

        user_properties_row = {}
        if user_properties_params:
            for param in user_properties_params:
                user_properties_row.update(param)
        element['user_properties'] = user_properties_row

        if 'user_properties' in element and isinstance(element['user_properties'], dict):
            if 'cid' in element['user_properties']:
                element['user_properties']['cid'] = str(element['user_properties']['cid'])

        yield element


def run():
    pipeline_options = ChosunGABigQueryOptions()
    pipeline = beam.Pipeline(options=pipeline_options)

    # Load schema from BigQuery table
    table_spec = pipeline_options.table_spec
    project_id, dataset_id, table_id = table_spec.split(':')[-1].split('.')
    schema_json = beam.io.gcp.bigquery.BigQuerySource.get_table_schema(
        project_id=project_id,
        dataset_id=dataset_id,
        table_id=table_id
    )
    table_schema = parse_table_schema_from_json(schema_json)

    # Read from BigQuery
    input_data = (pipeline
                  | 'Read from BigQuery' >> ReadFromBigQuery(table=pipeline_options.table_spec, method='DIRECT_READ')
                  | 'Process Data' >> beam.ParDo(ChosunGADataProcessing())
                  )

    # Write to BigQuery
    input_data | 'Write to BigQuery' >> WriteToBigQuery(
        table=pipeline_options.output,
        schema=table_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        method='STORAGE_WRITE_API',
        custom_gcs_temp_location=pipeline_options.custom_gcs_temp_location
    )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
