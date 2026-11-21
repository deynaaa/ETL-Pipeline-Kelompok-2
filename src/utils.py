"""
Helper & Konstanta Bersama — Pipeline ETL Multibahasa ChatGPT Twitter
Kelompok 2 — Proyek UAS Pemrograman 3
"""

from __future__ import annotations

import html
import logging
import re
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

try:
    import emoji as emoji_lib  # type: ignore

    HAS_EMOJI = True
except ImportError:
    emoji_lib = None
    HAS_EMOJI = False

MANDATORY_COLUMNS: List[str] = [
    "Datetime",
    "Tweet Id",
    "Text",
    "Language",
    "LikeCount",
    "RetweetCount",
]

ENGAGEMENT_COLUMNS: List[str] = [
    "LikeCount",
    "RetweetCount",
    "ReplyCount",
    "QuoteCount",
]

# Pemetaan lengkap seluruh 61 kode bahasa Twitter (ISO-639-1 & kode khusus Twitter) ke nama bahasa yang mudah dibaca
LANGUAGE_NAME_MAP: Dict[str, str] = {
    "am": "Amharic",
    "ar": "Arabic",
    "bg": "Bulgarian",
    "bn": "Bengali",
    "ca": "Catalan",
    "cs": "Czech",
    "cy": "Welsh",
    "da": "Danish",
    "de": "German",
    "el": "Greek",
    "en": "English",
    "es": "Spanish",
    "et": "Estonian",
    "eu": "Basque",
    "fa": "Persian",
    "fi": "Finnish",
    "fr": "French",
    "gu": "Gujarati",
    "hi": "Hindi",
    "ht": "Haitian Creole",
    "hu": "Hungarian",
    "hy": "Armenian",
    "id": "Indonesian",
    "in": "Indonesian",
    "is": "Icelandic",
    "it": "Italian",
    "iw": "Hebrew",
    "he": "Hebrew",
    "ja": "Japanese",
    "kn": "Kannada",
    "ko": "Korean",
    "lo": "Lao",
    "lt": "Lithuanian",
    "lv": "Latvian",
    "ml": "Malayalam",
    "mr": "Marathi",
    "ne": "Nepali",
    "nl": "Dutch",
    "no": "Norwegian",
    "or": "Odia",
    "pa": "Punjabi",
    "pl": "Polish",
    "pt": "Portuguese",
    "qam": "Mention Only",
    "qht": "Hashtag Only",
    "qme": "Media Only",
    "ro": "Romanian",
    "ru": "Russian",
    "sd": "Sindhi",
    "sl": "Slovenian",
    "sr": "Serbian",
    "sv": "Swedish",
    "ta": "Tamil",
    "te": "Telugu",
    "th": "Thai",
    "tl": "Tagalog",
    "tr": "Turkish",
    "uk": "Ukrainian",
    "und": "Undefined",
    "ur": "Urdu",
    "vi": "Vietnamese",
    "zh": "Chinese",
    "zxx": "No Linguistic Content",
}

URL_PATTERN = re.compile(r"https?://\S+|www\.\S+|t\.co/\S+", re.IGNORECASE)
MENTION_PATTERN = re.compile(r"@\w+")
RT_PREFIX_PATTERN = re.compile(r"^\s*RT\s+@\w+:?\s*|^\s*RT\s*:\s*", re.IGNORECASE)
RT_SIGNAL_PATTERN = re.compile(r"^\s*RT\s+@", re.IGNORECASE)
NEWLINE_PATTERN = re.compile(r"[\r\n]+")
WHITESPACE_PATTERN = re.compile(r"\s+")
LANG_SAFE_PATTERN = re.compile(r"[^a-z0-9]")

EMOJI_FALLBACK_PATTERN = re.compile(
    "["
    "\U0001F300-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\U00002700-\U000027BF"
    "\U00002600-\U000026FF"
    "\U0001F1E0-\U0001F1FF"
    "]",
    flags=re.UNICODE,
)


def get_language_name(code: str) -> str:
    """Mengubah kode bahasa (misal 'de', 'cs', 'in') menjadi nama bahasa yang mudah dibaca ('German', 'Czech', 'Indonesian')."""
    clean_code = LANG_SAFE_PATTERN.sub("", str(code).lower().strip())
    return LANGUAGE_NAME_MAP.get(clean_code, f"Other ({clean_code or 'unknown'})")


def get_language_file_slug(code: str) -> str:
    """Membuat slug nama file yang ramah dibaca: misal 'en' -> 'english_en', 'in' -> 'indonesian_in', 'cs' -> 'czech_cs'."""
    clean_code = LANG_SAFE_PATTERN.sub("", str(code).lower().strip()) or "unknown"
    readable = get_language_name(clean_code).lower()
    readable_slug = re.sub(r"[^a-z0-9]+", "_", readable).strip("_")
    return f"{readable_slug}_{clean_code}"


def make_run_folder_name(run_id: str) -> str:
    """Mengubah Airflow run_id menjadi nama folder aman & terurut kronologis."""
    ts_now = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    prefix = run_id.split("__")[0] if "__" in run_id else "run"
    safe_prefix = re.sub(r"[^a-zA-Z0-9_-]", "", prefix)[:20] or "run"
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})", run_id)
    if m:
        ts_part = f"{m.group(1)}{m.group(2)}{m.group(3)}_{m.group(4)}{m.group(5)}{m.group(6)}"
    else:
        ts_part = ts_now
    return f"{ts_part}__{safe_prefix}"


def setup_logger(log_file: str | Path | None = None, name: str = "chatgpt_twitter_etl") -> logging.Logger:
    """Menyiapkan logger terstruktur untuk CLI lokal (di Airflow memakai airflow.task)."""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

    if log_file is not None:
        lp = Path(log_file)
        lp.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(lp, encoding="utf-8")
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger


def count_emojis(text: str) -> int:
    if HAS_EMOJI:
        return int(emoji_lib.emoji_count(text))
    return len(EMOJI_FALLBACK_PATTERN.findall(text))


def demojize_text(text: str) -> str:
    if HAS_EMOJI:
        return emoji_lib.demojize(text, delimiters=(" :", ": "))
    return EMOJI_FALLBACK_PATTERN.sub(lambda m: f" :emoji_{ord(m.group(0)):x}: ", text)


def strip_emojis(text: str) -> str:
    if HAS_EMOJI:
        return emoji_lib.replace_emoji(text, replace=" ")
    return EMOJI_FALLBACK_PATTERN.sub(" ", text)


def build_base_clean_text(raw_text: str) -> str:
    s = html.unescape(str(raw_text))
    s = NEWLINE_PATTERN.sub(" ", s)
    s = RT_PREFIX_PATTERN.sub("", s)
    s = URL_PATTERN.sub(" ", s)
    s = MENTION_PATTERN.sub(" ", s)
    s = s.replace("#", "")
    return WHITESPACE_PATTERN.sub(" ", s).strip()


def build_text_context(base_clean: str) -> str:
    return WHITESPACE_PATTERN.sub(" ", demojize_text(base_clean)).strip()


def build_text_normalized(base_clean: str) -> str:
    s = strip_emojis(base_clean).lower()
    cleaned_chars = [
        ch if (unicodedata.category(ch)[0] in ("L", "M", "N") or ch.isspace()) else " "
        for ch in s
    ]
    return WHITESPACE_PATTERN.sub(" ", "".join(cleaned_chars)).strip()
