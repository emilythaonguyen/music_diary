"""
Text normalization and transliteration utilities.
Handles multiple scripts (Latin, Japanese, Korean, Chinese) and fuzzy matching.
"""
import re
import unicodedata

import pykakasi
from korean_romanizer.romanizer import Romanizer
from pypinyin import lazy_pinyin
from rapidfuzz import fuzz
from unidecode import unidecode


# initialize transliteration tools
_kks = pykakasi.kakasi()


def normalize_text(text: str) -> str:
    """Basic text normalization: strip and normalize whitespace."""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', text).strip()


def clean_artist_name(name: str) -> str:
    """Remove featuring tags and normalize whitespace."""
    if not name:
        return ""
    # remove everything after feat/ft
    name = re.split(r'\b(feat\.?|ft\.?)\b', name, flags=re.IGNORECASE)[0]
    return normalize_text(name)


def normalize_name(name: str) -> str:
    """
    Normalize text for comparison by removing versioning/remaster info.
    Used for fuzzy matching.
    """
    if not name:
        return ""
    
    name = unicodedata.normalize("NFKC", name).strip().lower()
    
    cruft = (
        r"remaster(ed)?|version|mix|edit|live|mono|stereo|bonus|session|take|"
        r"radio|acoustic|original|alternate|demo|instrumental|explicit|"
        r"anniversary|deluxe|expanded"
    )
    
    # remove trailing years and cruft
    name = re.sub(r"[\s]*[-–—:]+\s*\d{4}(\s+(" + cruft + r"))?.*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\(\s*\d{4}(\s+(" + cruft + r"))?\s*\)$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"[\s]*[-–—:]+\s*(" + cruft + r")\b.*$", "", name, flags=re.IGNORECASE)
    name = re.sub(r"\((\s*" + cruft + r"\s*)\)$", "", name, flags=re.IGNORECASE)
    
    # remove features
    name = re.sub(r"[\s\(\[]+(feat|ft|featuring|w\/|with)\s+.+?[\)\]]?$", "", name, flags=re.IGNORECASE)
    
    # clean punctuation
    name = re.sub(r"[^\w\s#\-]", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    
    return name


def normalize_for_validation(text: str) -> str:
    """
    Aggressive normalization for validation.
    Lowercase, remove ASCII punctuation and spaces, transliterate.
    """
    if not text:
        return ""
    
    text = transliterate(text).lower()
    # remove all ascii punctuation and whitespace
    text = re.sub(r"[^\w]", "", text)
    return text


def normalize_title(title: str, aggressive: bool = False) -> str:
    """
    Lightweight normalization for titles.
    Keeps non-Latin characters intact unless aggressive=True.
    """
    if not title:
        return ""
    
    title = title.strip().lower()
    
    if aggressive:
        # remove ascii brackets only
        title = re.sub(r"[\(\[\{].*?[\)\]\}]", "", title)
        # remove ascii punctuation only
        title = re.sub(r'[!"#$%&\'()*+,\-./:;<=>?@[\\\]^_`{|}~]', "", title)
        # normalize whitespace
        title = re.sub(r"\s+", " ", title)
    
    return title


def normalize_sort_name(name: str, artist_type: str) -> str:
    """
    Normalize sort name for artists.
    For Person type, swap "Last, First" to "First Last".
    """
    if not name:
        return ""
    
    if artist_type == "Person" and "," in name:
        last, first = name.split(",", 1)
        return f"{first.strip()} {last.strip()}"
    
    return name


def detect_script(text: str) -> str:
    """Detect if text is Japanese, Korean, Chinese, or Latin."""
    for ch in text:
        code = ord(ch)
        # korean (hangul)
        if 0xAC00 <= code <= 0xD7AF or 0x1100 <= code <= 0x11FF:
            return "ko"
        # japanese (hiragana/katakana)
        if 0x3040 <= code <= 0x30FF:
            return "ja"
        # chinese (cjk unified ideographs)
        if 0x4E00 <= code <= 0x9FFF:
            return "zh"
    return "latin"


def transliterate(text: str) -> str:
    """
    Transliterate non-Latin scripts to Latin.
    Supports Japanese, Korean, and Chinese.
    """
    if not text:
        return ""
    
    script = detect_script(text)
    
    try:
        if script == "ja":
            result = _kks.convert(text)
            return "".join(item['hepburn'] for item in result)
        if script == "ko":
            return Romanizer(text).romanize().replace(" ", "")
        if script == "zh":
            return "".join(lazy_pinyin(text))
    except Exception:
        pass
    
    # fallback to unidecode for other scripts
    return unidecode(text).replace(" ", "")


def similarity_score(a: str, b: str, method: str = "fuzzy") -> float:
    """
    Calculate similarity between two strings (0-1 scale).
    
    Args:
        a: First string
        b: Second string
        method: "fuzzy" for token-based or "validation" for aggressive normalization
    
    Returns:
        Similarity score from 0.0 to 1.0
    """
    if method == "fuzzy":
        a_norm = normalize_name(a)
        b_norm = normalize_name(b)
        
        set_score = fuzz.token_set_ratio(a_norm, b_norm)
        partial = fuzz.partial_ratio(a_norm, b_norm)
        
        return (0.6 * set_score + 0.4 * partial) / 100
    
    elif method == "validation":
        a_norm = normalize_for_validation(a)
        b_norm = normalize_for_validation(b)
        
        # catch symbolic textspace
        if not a_norm and not b_norm:
            a_basic = normalize_text(a).lower()
            b_basic = normalize_text(b).lower()
            if a_basic == b_basic and a_basic != "":
                return 1.0 
        
        if not a_norm or not b_norm:
            return 0.0
        
        scores = [
            fuzz.ratio(a_norm, b_norm),
            fuzz.partial_ratio(a_norm, b_norm),
            fuzz.token_sort_ratio(a_norm, b_norm)
        ]
        return max(scores) / 100
    
    else:
        raise ValueError(f"Unknown method: {method}")


def compute_fuzzy_score(db_norm: str, db_romaji: str, candidate: str) -> float:
    """
    Compute fuzzy score for validation, comparing both original and romanized forms.
    
    Args:
        db_norm: Normalized database text
        db_romaji: Romanized database text
        candidate: Candidate text from API
    
    Returns:
        Best score from 0.0 to 1.0
    """
    cand_norm = normalize_title(candidate, aggressive=True)
    cand_romaji = normalize_title(transliterate(candidate), aggressive=True)
    
    scores = [
        fuzz.ratio(db_norm, cand_norm),
        fuzz.ratio(db_romaji, cand_norm),
        fuzz.ratio(db_norm, cand_romaji),
        fuzz.ratio(db_romaji, cand_romaji)
    ]
    return max(scores) / 100