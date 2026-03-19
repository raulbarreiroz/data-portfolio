import re

def spaces_normalizer(text: str):
    return re.sub(r' {2,}', ' ', text)