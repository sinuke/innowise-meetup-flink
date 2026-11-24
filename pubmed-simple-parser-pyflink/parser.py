import gzip
import xml.parsers.expat

_CHUNK_SIZE = 1 << 16  # 64 KiB
_TEXT_TAGS = frozenset({'PMID', 'ArticleTitle', 'ISSN', 'ForeName', 'LastName', 'CollectiveName'})


class _PubMedHandler:
    """expat callbacks; finished records are appended to `pending` as (pmid, title, issn, authors)."""

    def __init__(self, pending: list):
        self.pending = pending
        self.capturing = None  # tag whose text is being collected; nested markup (<i>, <sup>) stays inside it
        self.buf = []
        self.in_medline_citation = False
        self.author = None  # name parts of the current <Author>, None outside of it
        self.pmid = self.title = self.issn = None
        self.authors = []

    def start(self, name, attrs):
        if name in _TEXT_TAGS:
            self.capturing = name
            self.buf.clear()
        elif name == 'MedlineCitation':
            self.in_medline_citation = True
        elif name == 'Author':
            self.author = {}

    def chars(self, data):
        if self.capturing:
            self.buf.append(data)

    def end(self, name):
        if name == self.capturing:
            self.capturing = None
            text = ''.join(self.buf).strip()
            if name == 'PMID':
                if self.in_medline_citation and self.pmid is None:
                    self.pmid = text
            elif name == 'ArticleTitle':
                self.title = text
            elif name == 'ISSN':
                if self.issn is None:
                    self.issn = text
            elif self.author is not None:  # ForeName / LastName / CollectiveName
                self.author[name] = text
        elif name == 'MedlineCitation':
            self.in_medline_citation = False
        elif name == 'Author':
            a = self.author
            full_name = f"{a.get('ForeName', '')} {a.get('LastName', '')}".strip() or a.get('CollectiveName', '')
            if full_name:
                self.authors.append(full_name)
            self.author = None
        elif name == 'PubmedArticle':
            self.pending.append((self.pmid or '', self.title or '', self.issn or '', self.authors))
            self.pmid = self.title = self.issn = None
            self.authors = []
            self.in_medline_citation = False


def _make_parser(pending: list):
    parser = xml.parsers.expat.ParserCreate()
    parser.buffer_text = True
    parser.DefaultHandler = lambda data: None
    parser.SetParamEntityParsing(xml.parsers.expat.XML_PARAM_ENTITY_PARSING_NEVER)
    parser.ExternalEntityRefHandler = lambda context, base, system_id, public_id: 0

    handler = _PubMedHandler(pending)
    parser.StartElementHandler = handler.start
    parser.CharacterDataHandler = handler.chars
    parser.EndElementHandler = handler.end
    return parser


def parse_pubmed_gz(file_path: str):
    pending: list = []
    parser = _make_parser(pending)

    with gzip.open(file_path, 'rb') as f:
        while True:
            chunk = f.read(_CHUNK_SIZE)
            is_final = not chunk
            parser.Parse(chunk, is_final)

            if pending:
                yield from pending
                pending.clear()

            if is_final:
                break
