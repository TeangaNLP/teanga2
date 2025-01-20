from base64 import b64encode
from hashlib import sha256


def teanga_id_for_doc(ids, *args, **kwargs):
    """Return the Teanga ID for a document.

    Parameters:
        ids: str
            The IDs already generated and not to be repeated

        This works as the add_doc method, but returns the Teanga ID for the document.
        It is not necessary to call this method directly but instead you can use it
        via the Corpus class.

    Examples:
        >>> teanga_id_for_doc(set(), text="This is a document.")
        'Kjco'
        >>> teanga_id_for_doc(set(), en="This is a document.", nl="Dit is een document.")
        'Nnrd'
    """
    text = ""
    if len(kwargs) == 0:
        raise Exception("No arguments given.")
    for key in sorted(kwargs.keys()):
        text += key
        text += "\x00"
        text += kwargs[key]
        text += "\x00"
    code = b64encode(sha256(text.encode("utf-8")).digest()).decode("utf-8")
    n = 4
    while code[:n] in ids and n < len(code):
        n += 1
    return code[:n]


def find_spans(tokens, text):
    """Find the spans of tokens in the text.

    Parameters:
        tokens: list
            A list of tokens to find in the text.
        text: str
            The text to search for the tokens.

    Exceptions:
        ValueError: If the tokens do not match the text.
    """
    i = 0
    tk_idx = 0
    spans = []
    while i < len(text) and tk_idx < len(text):
        if text[i:].startswith(tokens[tk_idx]):
            spans.append([i, i+len(tokens[tk_idx])])
            i += len(tokens[tk_idx])
            tk_idx += 1
        else:
            i += 1
    if tk_idx < len(tokens):
        raise ValueError("Tokenization mismatch")
    return spans
