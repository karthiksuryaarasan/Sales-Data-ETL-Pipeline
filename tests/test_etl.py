
from src.etl import extract, transform

def test_extract():
    df = extract()
    assert not df.empty

def test_transform():
    df = extract()
    df = transform(df)
    assert 'amount' in df.columns
