import glob
import gzip
import os
from lxml import etree
from pyflink.common import Types, Configuration, WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, ReduceFunction

PARALLELISM = 1
INPUT_DATA_PATH = '../data/pubmed_sample_120K_records/'
EXECUTION_MODE = 'process'
UI_PORT = '8081'

os.environ['no_proxy'] = "localhost,127.0.0.1"
os.environ['_python_worker_execution_mode'] = EXECUTION_MODE


def create_env(parallelism: int) -> StreamExecutionEnvironment:
    config = Configuration()
    config.set_string('rest.port', UI_PORT)
    config.set_string('python.execution-mode', EXECUTION_MODE)
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    return env.set_parallelism(parallelism)


def parse_pubmed_gz(file_path: str):
    with gzip.open(file_path, 'rb') as f:
        context = iter(etree.iterparse(f, events=('start', 'end')))
        _, root = next(context)

        for event, elem in context:
            if event == 'end' and elem.tag == 'PubmedArticle':

                pmid = ''
                title = ''
                issn = ''
                authors = []

                citation = elem.find('MedlineCitation')

                if citation is not None:
                    pmid_elem = citation.find('PMID')
                    if pmid_elem is not None and pmid_elem.text:
                        pmid = pmid_elem.text

                    article = citation.find('Article')
                    if article is not None:
                        title_elem = article.find('ArticleTitle')
                        if title_elem is not None:
                            title = ''.join(title_elem.itertext()).strip()

                        issn_elem = article.find('Journal/ISSN')
                        if issn_elem is not None and issn_elem.text:
                            issn = issn_elem.text

                        for author in article.findall('AuthorList/Author'):
                            fore = author.findtext('ForeName', default='').strip()
                            last = author.findtext('LastName', default='').strip()

                            if fore or last:
                                authors.append(f'{fore} {last}'.strip())
                            else:
                                collective = author.findtext('CollectiveName', default='').strip()
                                if collective:
                                    authors.append(collective)

                yield pmid, title, issn, authors

                elem.clear()
                root.clear()


class ReduceCount(ReduceFunction):

    def reduce(self, value1, value2):
        return value1[0], value1[1] + value2[1]


def main():
    env = create_env(PARALLELISM)
    directory_path = os.path.abspath(INPUT_DATA_PATH)
    file_paths = glob.glob(os.path.join(directory_path, '*.xml.gz'))

    schema = Types.TUPLE([
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.LIST(Types.STRING()),
    ])

    records_ds = env\
        .from_collection(collection=file_paths, type_info=Types.STRING())\
        .name("Get a list of raw GZ files")\
        .flat_map(parse_pubmed_gz, output_type=schema)\
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

    counted_ds.print().name("Print ISSNs with counts")

    result = env.execute('PubMed simple parser')

    net_runtime_ms = result.get_net_runtime()
    print('*'*60)
    print(f"Processing time: {net_runtime_ms / 1000.0:.2f} seconds")
    print('*'*60)


if __name__ == '__main__':
    main()
