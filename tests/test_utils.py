from teanga.utils import find_spans

def test_find_spans():
    tokens = ["Hello", "world", "!"]
    text = "Hello world!"
    spans = find_spans(tokens, text)
    assert spans == [[0, 5], [6, 11], [11, 12]]

    try:
        tokens = ["Hello", "word", "!"]
        text = "Hello world"
        spans = find_spans(tokens, text)
        assert False
    except ValueError:
        pass
