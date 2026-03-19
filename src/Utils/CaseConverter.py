from typing import List

def kehab_to_pascal(s: str) -> str:
    """
    Conver a kehab-case string to a pascal-case string.

    Args:
        s: Input string in kehab-case (e.g. 'a-arrow-up')
    
    Returns:
        str: Output string in pascal-case (e.g. 'AArrowUp')

    """
    parts: List[str] = [part for part in s.split('-') if part]
    return ''.join(part.capitalize() for part in parts)