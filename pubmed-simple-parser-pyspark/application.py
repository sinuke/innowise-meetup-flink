import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType


INPUT_DATA_PATH = '../input_raws/full_pubmed_baseline_2026/'
OUTPUT_DATA_BASE_PATH = '../output_csv/pyspark/'
PARALLELISM = 1
UI_PORT = '8083'


def create_spark(parallelism: int = 1, port: str = '0') -> SparkSession:
    return (
        SparkSession.builder
        .appName('PubMed simple parser')
        .master(f'local[{parallelism}]')
        .config('spark.ui.port', port)
        .config('spark.driver.host', '127.0.0.1')
        .config('spark.driver.bindAddress', '127.0.0.1')
        .getOrCreate()
    )


def append_timestamp(path: str) -> str:
    return f"{path}{'' if path.endswith('/') else '/'}{int(time.time() * 1000)}"


RECORD_SCHEMA = StructType([
    StructField('MedlineCitation', StructType([
        StructField('PMID', StructType([
            StructField('_VALUE', StringType()),
            StructField('_Version', StringType()),
        ])),
        StructField('Article', StructType([
            StructField('ArticleTitle', StringType()),
            StructField('Journal', StructType([
                StructField('ISSN', StructType([
                    StructField('_VALUE', StringType()),
                    StructField('_IssnType', StringType()),
                ])),
            ])),
            StructField('AuthorList', StructType([
                StructField('Author', ArrayType(StructType([
                    StructField('ForeName', StringType()),
                    StructField('LastName', StringType()),
                    StructField('CollectiveName', StringType()),
                ]))),
            ])),
        ])),
    ])),
])

AUTHORS_EXPR = """
    transform(
        coalesce(MedlineCitation.Article.AuthorList.Author, array()),
        author -> CASE
            WHEN coalesce(author.ForeName, '') != '' OR coalesce(author.LastName, '') != ''
                THEN trim(concat_ws(' ', coalesce(author.ForeName, ''), coalesce(author.LastName, '')))
            ELSE coalesce(author.CollectiveName, '')
        END
    ) AS authors
"""


def main():
    spark = create_spark(PARALLELISM, UI_PORT)

    input_dir = os.path.abspath(INPUT_DATA_PATH)
    output_path = append_timestamp(os.path.abspath(OUTPUT_DATA_BASE_PATH))

    raw_df = (
        spark.read
        .format('xml')
        .option('rowTag', 'PubmedArticle')
        .schema(RECORD_SCHEMA)
        .load(input_dir)
    )

    records_df = raw_df.selectExpr(
        'MedlineCitation.PMID._VALUE AS pmid',
        'MedlineCitation.Article.ArticleTitle AS title',
        'MedlineCitation.Article.Journal.ISSN._VALUE AS issn',
        AUTHORS_EXPR,
    )

    issn_df = records_df.select('issn').filter("issn IS NOT NULL AND issn != ''")

    counted_df = issn_df.groupBy('issn').count().select('issn', 'count')

    start = time.time()
    counted_df.coalesce(1).write.option('header', 'true').csv(output_path)
    elapsed = time.time() - start

    print('*' * 60)
    print(f"Processing time: {elapsed:.2f} seconds")
    print('*' * 60)

    spark.stop()


if __name__ == '__main__':
    main()
