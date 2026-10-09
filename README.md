# PubMed XML processing: Flink (Java) vs PyFlink vs PySpark

One task, three implementations, the same data:

> Read the full PubMed 2026 baseline (1334 `.xml.gz` files, ~51 GB compressed, ~300 GB of XML, ~40M articles), extract the journal ISSN of every article and count articles per ISSN.

[ISSN](https://www.issn.org/) (International Standard Serial Number) is the 8-character identifier of a periodical, e.g. `0028-0836` for *Nature*. Counting articles per ISSN effectively means counting articles per journal.

The repository was prepared for an internal Data Engineering meetup. The goal is to compare how the same batch job looks, runs and scales on **Apache Flink with Java**, **PyFlink** and **PySpark 4**.

## Results at a glance

Full dataset (39.5M articles with an ISSN), net processing time:

| | 1 thread (Intel) | 18 threads (Intel) | 1 thread (Ryzen) | 16 threads (Ryzen) |
|---|---|---|---|---|
| Flink + Java | 53.6 min | **10.7 min** | 67.3 min | **6.5 min** |
| PySpark | 59.3 min | 13.0 min | 60.4 min | 7.7 min |
| PyFlink | 3.1 h | 3.2 h | — | — |

- Intel: Core Ultra 5 125H, 40 GB DDR5-5600, NVMe PCIe 4.0.
- Ryzen: Ryzen 9 7940HS, 32 GB DDR5-5200, NVMe PCIe 4.0 (Samsung 980 Pro).

Java and PySpark scale 5–10x with parallelism. PyFlink does not scale at all. Flink has no built-in XML format, but its `StreamFormat` interface plus the JDK's standard StAX parser make a custom XML source easy to write in Java, and that source runs inside the JVM. PyFlink cannot plug a Python format into `FileSource`, so XML parsing runs in a Python UDF. In PyFlink's `thread` execution mode all subtasks of one JVM share a single CPython interpreter, and therefore a single GIL.

Raw numbers: [`statistics/PubMed simple parser statistics.xlsx`](statistics/). Flink/Spark UI screenshots: [`statistics/intel/`](statistics/intel/) and [`statistics/ryzen/`](statistics/ryzen/).

## Repository layout

| Path | What it is |
|---|---|
| [`pubmed-data-downloader/`](pubmed-data-downloader/) | Async downloader of the PubMed 2026 baseline with MD5 verification |
| [`pubmed-simple-parser-java/`](pubmed-simple-parser-java/) | Flink 2.3 job in Java 21: custom `StreamFormat` + StAX parser |
| [`pubmed-simple-parser-pyflink/`](pubmed-simple-parser-pyflink/) | The same job on the PyFlink DataStream API, XML parsed with `expat` |
| [`pubmed-simple-parser-pyspark/`](pubmed-simple-parser-pyspark/) | The same job on PySpark 4.2, using Spark's built-in XML data source |
| [`statistics/`](statistics/) | Timing spreadsheet and Flink/Spark UI screenshots |
| `input_raws/` | Input data (not in git, see below). Contains the PubMed DTD for reference |
| `output_csv/` | Job results (not in git, created on first run) |

All three jobs produce the same output: a CSV with header `issn,count`, written to `output_csv/<implementation>/<timestamp>/`.

## Prerequisites

| Tool | Version | Needed for |
|---|---|---|
| JDK | 21 recommended (the Java project targets 21). The jobs were run with JDK 25 | All three jobs: Flink and Spark run on the JVM, PyFlink and PySpark start a JVM under the hood |
| Python | **3.12.10** for all Python projects (pinned in `.python-version` and `requires-python = ">=3.12.10,<3.13"`). uv downloads it automatically if it is not installed | Downloader, PyFlink, PySpark |
| [uv](https://docs.astral.sh/uv/) | any recent | Python dependency management (`uv sync`, `uv run`) |
| Gradle | not needed, the wrapper (`gradlew`, Gradle 9.6) is included | Java job |
| IntelliJ IDEA (or another IDE with Gradle + Lombok support) | any recent | Running the Java job (see below) |
| ~60 GB free disk space | | Full dataset (51 GB) + outputs |

Make sure `java` is on `PATH` and/or `JAVA_HOME` points to the JDK.

### Windows-specific requirements

The project was developed and run on Windows 11.

- **PySpark** needs Hadoop's native Windows binaries (`winutils.exe`, `hadoop.dll`) to write files. Put them into a folder such as `C:\Users\<you>\.hadoop\bin` and set `HADOOP_HOME=C:\Users\<you>\.hadoop`.
- **PyFlink needs a C/C++ compiler on Windows.** Two dependencies have no Windows wheels on PyPI, so `uv sync` compiles them from source:
  - [`pemja`](https://pypi.org/project/pemja/): embeds CPython into the JVM, required by `thread` execution mode. It is compiled against the JDK's JNI headers, so `JAVA_HOME` must point to a JDK when you run `uv sync`.
  - [`crcmod`](https://pypi.org/project/crcmod/): a transitive dependency of Apache Beam, which PyFlink uses internally.

  Install **Visual Studio Build Tools** (tested with Build Tools 2026) with the **"Desktop development with C++"** workload before running `uv sync`. Without it the installation fails.
- **Keep `pemja` at version 0.5.7.** The Java side of pemja is bundled inside Flink 2.3 (`flink-python-2.3.0.jar` contains `com.alibaba:pemja:0.5.7`), so the Python package should be the same version. On Linux/macOS `apache-flink` pins it itself, but on Windows it does not declare `pemja` at all. That is why [`pyproject.toml`](pubmed-simple-parser-pyflink/pyproject.toml) pins it explicitly: `pemja==0.5.7; sys_platform == 'win32'`.
- **Use Python 3.12, not newer.** `apache-flink` 2.3 supports Python 3.9–3.12 and pins old versions of its dependencies (`apache-beam ≤ 2.61`, `pandas < 2.3`, `pyarrow < 21`). On Python 3.13+ these versions have no ready-made wheels, uv tries to build them from source, and the build fails. That is why every Python project has a `.python-version` file (`3.12.10`) and `requires-python = ">=3.12.10,<3.13"`: uv uses Python 3.12.10 even if a newer Python is installed. If you install dependencies without uv, create the virtual environment with Python 3.12 yourself.

### Linux / macOS

Not tested. Things to watch out for:

- `pemja` comes with `apache-flink` as a regular dependency with ready-made wheels, so a compiler should not be needed. Python 3.12 is still required.
- [`pubmed-simple-parser-pyflink/application.py`](pubmed-simple-parser-pyflink/application.py) adds `<venv>/Lib/site-packages` to `PYTHONPATH`, which is the Windows venv layout. On Linux/macOS the path is `<venv>/lib/python3.12/site-packages`. Adjust it if the embedded interpreter cannot import `pyflink`.
- `HADOOP_HOME` / `winutils` are not needed.

## 1. Get the data

Data files are not stored in git (`*.gz` is ignored).

### Full dataset (~51 GB)

```bash
cd pubmed-data-downloader
uv sync
uv run python downloader.py
```

This downloads `pubmed26n0001.xml.gz` … `pubmed26n1334.xml.gz` and their `.md5` files from `https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/` into `input_raws/full_pubmed_baseline_2026/`, five files at a time. Each archive is checked against its MD5. The script is safe to rerun: existing files are skipped and re-verified.

> **Note:** NCBI replaces the baseline once a year (usually around December–January). After the 2027 baseline is published, the `pubmed26n*` files will disappear from the FTP. The file prefix (`pubmed26n`) and the file count (1334) are hardcoded in [`downloader.py`](pubmed-data-downloader/downloader.py) and need to be updated then.

### Small sample (4 files, ~35 MB, ~115K articles)

Good for a quick check that everything works: each job takes seconds. The sample is just four files from the baseline:

```bash
mkdir -p input_raws/pubmed_sample_120K_records
cd input_raws/pubmed_sample_120K_records
for n in 0009 0176 0332 0457; do
  curl -O https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed26n$n.xml.gz
done
```

If the full dataset is already downloaded, copy these four files from `input_raws/full_pubmed_baseline_2026/` instead.

Expected result on the sample: 3 202 rows (ISSNs), counts summing to 114 728.

## 2. Configure the jobs

There are no command-line arguments. Input path, parallelism and the web UI port are constants at the top of each entry point. Switch between the sample (`../input_raws/pubmed_sample_120K_records/`) and the full dataset (`../input_raws/full_pubmed_baseline_2026/`) by editing them. In the Python scripts both paths are already there, one of them commented out.

| Job | File | Constants |
|---|---|---|
| Java | [`Main.java`](pubmed-simple-parser-java/src/main/java/com/innowise/pubmed/simple/parser/Main.java) | `INPUT_DATA_PATH`, `PARALLELISM`, `PORT` (8082) |
| PyFlink | [`application.py`](pubmed-simple-parser-pyflink/application.py) | `INPUT_DATA_PATH`, `PARALLELISM`, `UI_PORT` (8081), `EXECUTION_MODE` |
| PySpark | [`application.py`](pubmed-simple-parser-pyspark/application.py) | `INPUT_DATA_PATH`, `PARALLELISM`, `UI_PORT` (8083) |

Paths are relative (`../input_raws/...`, `../output_csv/...`), so **every job must be started from its own sub-project directory**.

## 3. Run

Each job prints its processing time at the end. While a job is running, its web UI is available at the port above (Flink UI for the Java and PyFlink jobs, Spark UI for PySpark).

### Flink + Java

1. Open `pubmed-simple-parser-java/` in IntelliJ IDEA as a Gradle project and let it import the dependencies.
2. Run `com.innowise.pubmed.simple.parser.Main` with the working directory set to `pubmed-simple-parser-java/` (IntelliJ's default for Gradle projects).
3. Flink UI: http://localhost:8082

The job runs on a local embedded Flink cluster (MiniCluster), so no Flink installation is needed. `./gradlew build` compiles the project from the command line, but there is no `run` task, so the job is started from the IDE.

### PyFlink

```bash
cd pubmed-simple-parser-pyflink
uv sync                     # Windows: needs Visual Studio Build Tools, see Prerequisites
uv run python application.py
```

Flink UI: http://localhost:8081. The job runs on a local MiniCluster started by PyFlink.

`EXECUTION_MODE = 'thread'` is the default and is ~20x faster than PyFlink's own default `'process'` on this job. Do not change `_python_worker_execution_mode`: it is an unrelated internal setting and must stay `'process'`. See [`CLAUDE.md`](CLAUDE.md) for details.

### PySpark

```bash
cd pubmed-simple-parser-pyspark
uv sync
uv run python application.py
```

Spark UI: http://localhost:8083 (only while the job is running). Spark runs in local mode (`local[PARALLELISM]`), no Spark installation is needed.

## 4. Check the result

```bash
cd output_csv/<java|pyflink|pyspark>/<timestamp>
cat $(find . -type f ! -name '.*' ! -name '_SUCCESS') | grep -v '^issn' | awk -F, '{s+=$2; n++} END {print n " ISSNs, " s " articles"}'
```

Expected values:

| Dataset | ISSNs | Articles with an ISSN |
|---|---|---|
| Sample | 3 202 | 114 728 |
| Full baseline 2026 | 38 326 | 39 536 019 |

## More details

- [`CLAUDE.md`](CLAUDE.md): architecture of each implementation and the PyFlink performance investigation.
- [`statistics/`](statistics/): timings and UI screenshots.
