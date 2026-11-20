# Обработка PubMed XML: Flink (Java) vs PyFlink vs PySpark

Одна задача, три реализации, одни и те же данные:

> Прочитать полный PubMed baseline 2026 (1334 файла `.xml.gz`, ~51 ГБ в сжатом виде, ~380 ГБ XML, ~40 млн статей), извлечь ISSN журнала для каждой статьи и посчитать количество статей на каждый ISSN.

[ISSN](https://www.issn.org/) (International Standard Serial Number) — 8-символьный идентификатор периодического издания, например `0028-0836` у *Nature*. Подсчёт статей по ISSN — по сути подсчёт статей по журналам.

Репозиторий подготовлен для внутреннего митапа по Data Engineering. Цель — сравнить, как одна и та же batch-задача выглядит, работает и масштабируется на **Apache Flink с Java**, **PyFlink** и **PySpark 4**.

## Результаты вкратце

Полный датасет (39,5 млн статей с ISSN), чистое время обработки:

| | 1 поток (Intel) | 18 потоков (Intel) | 1 поток (AMD) | 16 потоков (AMD) |
|---|---|---|---|---|
| Flink + Java | 47 мин 19 с | **7 мин 34 с** | 46 мин 57 с | **4 мин 56 с** |
| PySpark | 1 ч 12 мин 42 с | 14 мин 29 с | 59 мин 15 с | 7 мин 14 с |
| PyFlink | 2 ч 07 мин 36 с | — | 2 ч 04 мин 27 с | 2 ч 41 мин 05 с |

`—` — не измерялось.

- Intel: Core Ultra 5 125H, 40 ГБ DDR5-5600, NVMe PCIe 4.0.
- AMD: Ryzen 9 7940HS, 32 ГБ DDR5-5200, NVMe PCIe 4.0.

Java и PySpark ускоряются в 5–10 раз при увеличении параллелизма (Java: в 6,3 раза на Intel и в 9,5 раза на Ryzen; PySpark: в 5 раз на Intel и в 8,2 раза на Ryzen). PyFlink не масштабируется совсем: на Ryzen с 16 потоками он работал даже медленнее, чем с одним. Встроенного XML-формата во Flink нет, но интерфейс `StreamFormat` вместе со стандартным StAX-парсером из JDK позволяет легко написать собственный XML-источник на Java, и этот источник работает внутри JVM. PyFlink не умеет подключать Python-формат к `FileSource`, поэтому разбор XML выполняется в Python UDF. В режиме выполнения `thread` все подзадачи одной JVM используют один общий интерпретатор CPython, а значит, и один общий GIL.

## Структура репозитория

| Путь | Что это |
|---|---|
| [`pubmed-data-downloader/`](pubmed-data-downloader/) | Асинхронный загрузчик PubMed baseline 2026 с проверкой MD5 |
| [`pubmed-simple-parser-java/`](pubmed-simple-parser-java/) | Задача на Flink 2.3 и Java 21: собственный `StreamFormat` + StAX-парсер |
| [`pubmed-simple-parser-pyflink/`](pubmed-simple-parser-pyflink/) | Та же задача на PyFlink DataStream API, XML разбирается через `expat` |
| [`pubmed-simple-parser-pyspark/`](pubmed-simple-parser-pyspark/) | Та же задача на PySpark 4.2 со встроенным XML-источником Spark |
| `input_raws/` | Входные данные (не в git, см. ниже). Для справки здесь лежит DTD PubMed |
| `output_csv/` | Результаты задач (не в git, создаётся при первом запуске) |

Все три задачи выдают одинаковый результат: CSV с заголовком `issn,count`, который записывается в `output_csv/<реализация>/<timestamp>/`.

## Что нужно установить

| Инструмент | Версия | Для чего |
|---|---|---|
| JDK | рекомендуется 21 (Java-проект собирается под 21). Замеры делались на JDK 25 | Все три задачи: Flink и Spark работают на JVM, PyFlink и PySpark запускают JVM внутри |
| Python | **3.12.10** для всех Python-проектов (зафиксирован в `.python-version` и `requires-python = ">=3.12.10,<3.13"`). Если он не установлен, uv скачает его сам | Загрузчик, PyFlink, PySpark |
| [uv](https://docs.astral.sh/uv/) | любая свежая | Управление Python-зависимостями (`uv sync`, `uv run`) |
| Gradle | не нужен, wrapper (`gradlew`, Gradle 9.6) уже в репозитории | Java-задача |
| IntelliJ IDEA (или другая IDE с поддержкой Gradle и Lombok) | любая свежая | Запуск Java-задачи (см. ниже) |
| ~60 ГБ свободного места на диске | | Полный датасет (51 ГБ) + результаты |

Убедитесь, что `java` есть в `PATH` и/или `JAVA_HOME` указывает на JDK.

### Особенности Windows

Проект разрабатывался и запускался на Windows 11.

- **PySpark** для записи файлов нужны нативные бинарники Hadoop под Windows (`winutils.exe`, `hadoop.dll`). Положите их в папку вроде `C:\Users\<you>\.hadoop\bin` и задайте `HADOOP_HOME=C:\Users\<you>\.hadoop`.
- **PyFlink на Windows нужен компилятор C/C++.** У двух зависимостей нет wheel-пакетов под Windows на PyPI, поэтому `uv sync` собирает их из исходников:
  - [`pemja`](https://pypi.org/project/pemja/): встраивает CPython в JVM, нужен для режима выполнения `thread`. Компилируется с JNI-заголовками из JDK, поэтому при запуске `uv sync` переменная `JAVA_HOME` должна указывать на JDK.
  - [`crcmod`](https://pypi.org/project/crcmod/): транзитивная зависимость Apache Beam, который PyFlink использует внутри.

  Перед `uv sync` установите **Visual Studio Build Tools** (проверено на Build Tools 2026) с рабочей нагрузкой **«Разработка классических приложений на C++»** («Desktop development with C++»). Без неё установка завершится ошибкой.
- **Оставляйте `pemja` версии 0.5.7.** Java-часть pemja уже встроена во Flink 2.3 (`flink-python-2.3.0.jar` содержит `com.alibaba:pemja:0.5.7`), поэтому Python-пакет должен быть той же версии. На Linux/macOS `apache-flink` сам фиксирует эту версию, а на Windows вообще не объявляет `pemja` в зависимостях. Поэтому в [`pyproject.toml`](pubmed-simple-parser-pyflink/pyproject.toml) она зафиксирована явно: `pemja==0.5.7; sys_platform == 'win32'`.
- **Используйте Python 3.12, не новее.** `apache-flink` 2.3 поддерживает Python 3.9–3.12 и фиксирует старые версии своих зависимостей (`apache-beam ≤ 2.61`, `pandas < 2.3`, `pyarrow < 21`). Для Python 3.13+ готовых wheel-пакетов этих версий нет, uv пытается собрать их из исходников, и сборка падает. Поэтому в каждом Python-проекте есть файл `.python-version` (`3.12.10`) и `requires-python = ">=3.12.10,<3.13"`: uv использует Python 3.12.10, даже если установлена более новая версия. Если ставите зависимости без uv, создайте виртуальное окружение на Python 3.12 сами.

### Linux / macOS

Не проверялось. На что обратить внимание:

- `pemja` приходит вместе с `apache-flink` как обычная зависимость с готовыми wheel-пакетами, так что компилятор не должен понадобиться. Python 3.12 всё равно обязателен.
- [`pubmed-simple-parser-pyflink/application.py`](pubmed-simple-parser-pyflink/application.py) добавляет в `PYTHONPATH` путь `<venv>/Lib/site-packages` — это структура venv на Windows. На Linux/macOS путь будет `<venv>/lib/python3.12/site-packages`. Поправьте его, если встроенный интерпретатор не может импортировать `pyflink`.
- `HADOOP_HOME` и `winutils` не нужны.

## 1. Скачать данные

Файлы данных в git не хранятся (`*.gz` в игноре). Полный датасет занимает ~51 ГБ.

```bash
cd pubmed-data-downloader
uv sync
uv run python downloader.py
```

Скрипт скачивает `pubmed26n0001.xml.gz` … `pubmed26n1334.xml.gz` и их `.md5`-файлы с `https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/` в `input_raws/full_pubmed_baseline_2026/`, по пять файлов одновременно. Каждый архив проверяется по MD5. Скрипт можно безопасно перезапускать: уже скачанные файлы пропускаются и проверяются заново.

> **Примечание:** NCBI обновляет baseline раз в год (обычно в декабре–январе). После публикации baseline 2027 файлы `pubmed26n*` исчезнут с FTP. Префикс файлов (`pubmed26n`) и их количество (1334) зашиты в [`downloader.py`](pubmed-data-downloader/downloader.py) — тогда их нужно будет обновить.

## 2. Настроить задачи

Аргументов командной строки нет. Путь к входным данным (по умолчанию `../input_raws/full_pubmed_baseline_2026/`), параллелизм и порт веб-интерфейса задаются константами в начале каждой точки входа.

| Задача | Файл | Константы |
|---|---|---|
| Java | [`Main.java`](pubmed-simple-parser-java/src/main/java/com/innowise/pubmed/simple/parser/Main.java) | `INPUT_DATA_PATH`, `PARALLELISM`, `PORT` (8082) |
| PyFlink | [`application.py`](pubmed-simple-parser-pyflink/application.py) | `INPUT_DATA_PATH`, `PARALLELISM`, `UI_PORT` (8081), `EXECUTION_MODE` |
| PySpark | [`application.py`](pubmed-simple-parser-pyspark/application.py) | `INPUT_DATA_PATH`, `PARALLELISM`, `UI_PORT` (8083) |

Пути относительные (`../input_raws/...`, `../output_csv/...`), поэтому **каждую задачу нужно запускать из директории её подпроекта**.

## 3. Запустить

В конце каждая задача выводит время обработки. Пока задача работает, её веб-интерфейс доступен на указанном выше порту (Flink UI для Java и PyFlink, Spark UI для PySpark).

### Flink + Java

1. Откройте `pubmed-simple-parser-java/` в IntelliJ IDEA как Gradle-проект и дождитесь импорта зависимостей.
2. Запустите `com.innowise.pubmed.simple.parser.Main` с рабочей директорией `pubmed-simple-parser-java/` (для Gradle-проектов в IntelliJ это значение по умолчанию).
3. Flink UI: http://localhost:8082

Задача выполняется на локальном встроенном кластере Flink (MiniCluster), устанавливать Flink не нужно. `./gradlew build` собирает проект из командной строки, но задачи `run` нет, поэтому задача запускается из IDE.

### PyFlink

```bash
cd pubmed-simple-parser-pyflink
uv sync                     # Windows: нужны Visual Studio Build Tools, см. «Что нужно установить»
uv run python application.py
```

Flink UI: http://localhost:8081. Задача выполняется на локальном MiniCluster, который запускает PyFlink.

По умолчанию стоит `EXECUTION_MODE = 'thread'`: на этой задаче он в ~20 раз быстрее, чем стандартный для PyFlink режим `'process'`. Не меняйте `_python_worker_execution_mode`: это не связанная с ним внутренняя настройка, она должна оставаться `'process'`. Подробности — в [`CLAUDE.md`](CLAUDE.md).

### PySpark

```bash
cd pubmed-simple-parser-pyspark
uv sync
uv run python application.py
```

Spark UI: http://localhost:8083 (только пока задача работает). Spark работает в локальном режиме (`local[PARALLELISM]`), устанавливать Spark не нужно.

## 4. Проверить результат

```bash
cd output_csv/<java|pyflink|pyspark>/<timestamp>
cat $(find . -type f ! -name '.*' ! -name '_SUCCESS') | grep -v '^issn' | awk -F, '{s+=$2; n++} END {print n " ISSNs, " s " articles"}'
```

Ожидаемый результат на полном baseline 2026: 38 326 ISSN, 39 536 019 статей с ISSN.
