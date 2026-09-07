import gzip
import xml.parsers.expat

_CHUNK_SIZE = 1 << 16  # 64 KiB


def _make_parser(pending: list):
    parser = xml.parsers.expat.ParserCreate()
    parser.buffer_text = True
    parser.DefaultHandler = lambda data: None
    parser.SetParamEntityParsing(xml.parsers.expat.XML_PARAM_ENTITY_PARSING_NEVER)
    parser.ExternalEntityRefHandler = lambda context, base, system_id, public_id: 0

    state = {
        'pmid': None, 'title': None, 'issn': None, 'authors': [],
        'fore_name': '', 'last_name': '', 'collective_name': '',
        'in_medline_citation': False, 'in_article_title': False, 'in_author': False,
        'text_buf': [], 'title_buf': [],
    }

    def reset_record_state():
        state.update(
            pmid=None, title=None, issn=None, authors=[],
            fore_name='', last_name='', collective_name='',
            in_medline_citation=False, in_article_title=False, in_author=False,
        )

    def start_element(name, attrs):
        state['text_buf'] = []
        if name == 'MedlineCitation':
            state['in_medline_citation'] = True
        elif name == 'ArticleTitle':
            state['in_article_title'] = True
            state['title_buf'] = []
        elif name == 'Author':
            state['in_author'] = True
            state['fore_name'] = ''
            state['last_name'] = ''
            state['collective_name'] = ''

    def char_data(data):
        state['text_buf'].append(data)
        if state['in_article_title']:
            state['title_buf'].append(data)

    def end_element(name):
        text = ''.join(state['text_buf']).strip()

        if name == 'MedlineCitation':
            state['in_medline_citation'] = False
        elif name == 'PMID':
            if state['in_medline_citation'] and state['pmid'] is None:
                state['pmid'] = text
        elif name == 'ArticleTitle':
            state['in_article_title'] = False
            state['title'] = ''.join(state['title_buf']).strip()
        elif name == 'ISSN':
            if state['issn'] is None:
                state['issn'] = text
        elif name == 'ForeName':
            if state['in_author']:
                state['fore_name'] = text
        elif name == 'LastName':
            if state['in_author']:
                state['last_name'] = text
        elif name == 'CollectiveName':
            if state['in_author']:
                state['collective_name'] = text
        elif name == 'Author':
            state['in_author'] = False
            if state['fore_name'] or state['last_name']:
                state['authors'].append(f"{state['fore_name']} {state['last_name']}".strip())
            elif state['collective_name']:
                state['authors'].append(state['collective_name'])
        elif name == 'PubmedArticle':
            pending.append((
                state['pmid'] or '',
                state['title'] or '',
                state['issn'] or '',
                state['authors'],
            ))
            reset_record_state()

    parser.StartElementHandler = start_element
    parser.CharacterDataHandler = char_data
    parser.EndElementHandler = end_element
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