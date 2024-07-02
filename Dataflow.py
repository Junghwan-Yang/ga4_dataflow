import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json


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

    table_spec = pipeline_options.table_spec
    project_id = "ga4-link-2022"
    dataset_id = "analytics_310278593"
    table_id = "events_20240701"

    # Use BigQueryIO.Read.from_query to get the table schema
    table_schema_query = f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}` LIMIT 0"
    table_schema = (pipeline
                    | beam.io.Read(
                ReadFromBigQuery(
                    query=table_schema_query,
                    use_standard_sql=True
                )
            )
                    | beam.Map(lambda x: x.schema)
                    | beam.combiners.ToList()
                    | beam.Map(lambda schemas: parse_table_schema_from_json(str(schemas[0]))))
    table_schema = beam.pvalue.AsSingleton(table_schema)

    # Read data from BigQuery
    input_data = (pipeline
                  | 'Read from BigQuery' >> beam.io.Read(
                ReadFromBigQuery(
                    table=pipeline_options.table_spec,
                    use_standard_sql=True
                )
            )
                  | 'Process Data' >> beam.ParDo(ChosunGADataProcessing())
                  )

    # Write to BigQuery
    input_data | 'Write to BigQuery' >> WriteToBigQuery(
        table=pipeline_options.output,
        schema=table_schema,
        custom_gcs_temp_location=pipeline_options.custom_gcs_temp_location
    )

    result = pipeline.run()
    result.wait_until_finish()


if __name__ == '__main__':
    run()
