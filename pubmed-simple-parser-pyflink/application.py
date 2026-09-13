import glob
import os
import sys
import time
from pyflink.common import Types, Configuration, Row
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, ReduceFunction
from pyflink.datastream.connectors.file_system import FileSink, BucketAssigner
from pyflink.datastream.formats.csv import CsvSchema, CsvBulkWriters
from pyflink.table.types import DataTypes
from parser import parse_pubmed_gz


PARALLELISM = 0
# INPUT_DATA_PATH = '../input_raws/full_pubmed_baseline_2026/'
INPUT_DATA_PATH = '../input_raws/pubmed_sample_120K_records/'
OUTPUT_DATA_BASE_PATH = '../output_csv/pyflink/'
EXECUTION_MODE = 'thread'
UI_PORT = '8081'

os.environ['no_proxy'] = "localhost,127.0.0.1"
os.environ['_python_worker_execution_mode'] = 'process'

if EXECUTION_MODE == 'thread':
    venv_site_packages = os.path.join(sys.prefix, 'Lib', 'site-packages')
    os.environ['PYTHONPATH'] = os.pathsep.join(
        filter(None, [venv_site_packages, os.environ.get('PYTHONPATH')])
    )


def create_env(parallelism: int = 0, port: int = 0) -> StreamExecutionEnvironment:
    config = Configuration()
    if port != 0:
        config.set_string('rest.port', port)
    config.set_string('python.execution-mode', EXECUTION_MODE)
    config.set_string('jobmanager.scheduler', 'Default')
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    if parallelism != 0:
        env.set_parallelism(parallelism)
    return env


def append_timestamp(path: str) -> str:
    return f"{path}{'' if path.endswith('/') else '/'}{int(time.time() * 1000)}"


class ReduceCount(ReduceFunction):

    def reduce(self, value1, value2):
        return value1[0], value1[1] + value2[1]


def main():
    env = create_env(PARALLELISM, UI_PORT)
    directory_path = os.path.abspath(INPUT_DATA_PATH)
    file_paths = glob.glob(os.path.join(directory_path, '*.xml.gz'))
    output_path = append_timestamp(os.path.abspath(OUTPUT_DATA_BASE_PATH))
    
    records_ds = env\
        .from_collection(collection=file_paths, type_info=Types.STRING())\
        .name("Get a list of raw GZ files")\
        .flat_map(
            parse_pubmed_gz,
            output_type=Types.TUPLE([
                Types.STRING(),
                Types.STRING(),
                Types.STRING(),
                Types.LIST(Types.STRING()),
                ])
        )\
        .name("Parse PubMed records from raw XML files")

    # count issns
    counted_ds = records_ds\
        .filter(lambda rec: rec[2] != '')\
        .name("Filter out records without ISSN values")\
        .map(lambda rec: (rec[2], 1), Types.TUPLE([Types.STRING(), Types.INT()]))\
        .name("Create a tuples with ISSN value and 1 to make counts")\
        .key_by(lambda rec: rec[0])\
        .reduce(ReduceCount())\
        .name("Count records for each unique ISSN")

    csv_ready_ds = counted_ds\
        .map(lambda rec: Row(issn=rec[0], count=rec[1]),
             Types.ROW_NAMED(['issn', 'count'], [Types.STRING(), Types.INT()]))\
        .name("Convert tuples into rows for CSV")

    csv_schema = CsvSchema.builder()\
        .add_string_column('issn')\
        .add_number_column('count', number_type=DataTypes.INT())\
        .set_use_header()\
        .build()

    sink = FileSink\
        .for_bulk_format(output_path, CsvBulkWriters.for_schema(csv_schema))\
        .with_bucket_assigner(BucketAssigner.base_path_bucket_assigner())\
        .build()

    csv_ready_ds.sink_to(sink).name("Saving results as CSV on disk")

    result = env.execute('PubMed simple parser')

    net_runtime_ms = result.get_net_runtime()
    print('*'*60)
    print(f"Processing time: {net_runtime_ms / 1000.0:.2f} seconds")
    print('*'*60)


if __name__ == '__main__':
    main()
