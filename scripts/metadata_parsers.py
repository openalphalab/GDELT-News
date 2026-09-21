"""Only allowlisted derived fields survive parsing. Raw article bodies never enter state."""

import csv
import gzip
import html
import io
import json
import math
import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlsplit

from urllib.parse import parse_qsl, urlencode, urlunsplit

def canonical_url(url: str):
    p = urlsplit(url.strip())
    if p.scheme not in {"http", "https"} or not p.hostname or p.username or p.password:
        raise ValueError("Invalid publisher URL")
    # Query values may identify articles; remove only known tracking parameters.
    query = [
        (k, v)
        for k, v in parse_qsl(p.query, keep_blank_values=True)
        if not k.lower().startswith("utm_") and k.lower() not in {"fbclid", "gclid"}
    ]
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path or "/", urlencode(sorted(query)), ""))

# GKG extras can exceed Python's 128 KiB CSV default. Keep the existing bounded
# five-million-character record limit instead of rejecting the entire archive.
csv.field_size_limit(5_000_000)

LANGUAGES = {
    "spa": "Spanish",
    "fra": "French",
    "por": "Portuguese",
    "rus": "Russian",
    "zho": "Chinese",
    "ara": "Arabic",
    "ind": "Indonesian",
    "eng": "English",
    "tur": "Turkish",
    "deu": "German",
    "ita": "Italian",
    "jpn": "Japanese",
    "ces": "Czech",
    "dan": "Danish",
    "nld": "Dutch",
    "fin": "Finnish",
    "ell": "Greek",
    "heb": "Hebrew",
    "hin": "Hindi",
    "hun": "Hungarian",
    "kor": "Korean",
    "nor": "Norwegian",
    "pol": "Polish",
    "ron": "Romanian",
    "slk": "Slovak",
    "swe": "Swedish",
    "tha": "Thai",
    "ukr": "Ukrainian",
    "urd": "Urdu",
    "vie": "Vietnamese",
    "bul": "Bulgarian",
    "bos": "Bosnian",
    "hrv": "Croatian",
    "srp": "Serbian",
    "mkd": "Macedonian",
    "sqi": "Albanian",
    "fas": "Persian",
    "guj": "Gujarati",
    "mal": "Malayalam",
    "tam": "Tamil",
    "tel": "Telugu",
    "ben": "Bengali",
    "cat": "Catalan",
    "est": "Estonian",
    "lav": "Latvian",
    "lit": "Lithuanian",
    "slv": "Slovenian",
    "msa": "Malay",
    "en": "English",
    "es": "Spanish",
    "fr": "French",
    "de": "German",
    "pt": "Portuguese",
    "ru": "Russian",
    "zh": "Chinese",
    "ar": "Arabic",
    "ja": "Japanese",
    "ko": "Korean",
    "it": "Italian",
    "tr": "Turkish",
    "nl": "Dutch",
    "uk": "Ukrainian",
    "pl": "Polish",
    "he": "Hebrew",
    "id": "Indonesian",
    "hi": "Hindi",
    "vi": "Vietnamese",
    "th": "Thai",
}
LANGUAGES.update(
    {
        "af": "Afrikaans",
        "az": "Azerbaijani",
        "be": "Belarusian",
        "bg": "Bulgarian",
        "bh": "Bihari",
        "bn": "Bengali",
        "br": "Breton",
        "bs": "Bosnian",
        "ca": "Catalan",
        "co": "Corsican",
        "cs": "Czech",
        "da": "Danish",
        "el": "Greek",
        "eo": "Esperanto",
        "et": "Estonian",
        "eu": "Basque",
        "fa": "Persian",
        "fi": "Finnish",
        "gl": "Galician",
        "gu": "Gujarati",
        "hr": "Croatian",
        "ht": "Haitian Creole",
        "hu": "Hungarian",
        "hy": "Armenian",
        "iw": "Hebrew",
        "la": "Latin",
        "lt": "Lithuanian",
        "lv": "Latvian",
        "mk": "Macedonian",
        "ml": "Malayalam",
        "mn": "Mongolian",
        "mr": "Marathi",
        "ms": "Malay",
        "nn": "Norwegian Nynorsk",
        "no": "Norwegian",
        "rm": "Romansh",
        "ro": "Romanian",
        "sk": "Slovak",
        "sl": "Slovenian",
        "sq": "Albanian",
        "sr": "Serbian",
        "sv": "Swedish",
        "sw": "Swahili",
        "ta": "Tamil",
        "te": "Telugu",
        "tlh": "Klingon",
        "ur": "Urdu",
        "uz": "Uzbek",
        "un": "Unknown",
        "zh-hant": "Chinese",
        "chineset": "Chinese",
        "norwegian_n": "Norwegian Nynorsk",
        "waray_philippines": "Waray",
        "ne": "Nepali",
        "km": "Khmer",
        "ps": "Pashto",
        "ny": "Nyanja",
        "mg": "Malagasy",
        "is": "Icelandic",
    }
)


def normalize_language(value):
    raw = str(value or "").strip()
    if not raw or raw.lower() in {"unknown", "und", "un"}:
        return "Unknown"
    return LANGUAGES.get(raw.lower(), raw.replace("_", " ").title())[:60]


# GDELT location codes are FIPS, not ISO. Never pass unmapped values off as ISO.
FIPS_ISO = {
    "PE": "PE",
    "ID": "ID",
    "SN": "SN",
    "CH": "CN",
    "GM": "DE",
    "UK": "GB",
    "US": "US",
    "FR": "FR",
    "BR": "BR",
    "CI": "CL",
    "MX": "MX",
    "TU": "TR",
    "SF": "ZA",
    "IN": "IN",
    "JA": "JP",
    "AS": "AU",
    "CA": "CA",
    "RS": "RU",
    "UP": "UA",
    "EG": "EG",
    "KE": "KE",
    "NI": "NG",
    "RP": "PH",
    "VM": "VN",
    "TH": "TH",
    "IT": "IT",
    "SP": "ES",
    "NL": "NL",
}
# GeoNames countryInfo.txt snapshot; CC BY 4.0, see NOTICE.md.
FIPS_ISO.update(json.loads(Path(__file__).with_name("fips-iso.json").read_text(encoding="utf-8-sig")))


def timestamp(value):
    raw = str(value)
    if re.fullmatch(r"\d{14}", raw):
        return datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


def number(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (ValueError, TypeError):
        return None


def clean(value, limit=500):
    return re.sub(r"\s+", " ", html.unescape(str(value or ""))).strip()[:limit]


def extra_field(extras, name, limit=500):
    match = re.search(rf"<{name}>(.*?)</{name}>", extras, re.S)
    return clean(match[1], limit) if match else ""


def source_language(translation):
    match = re.search(r"srclc:([a-z]{3})", translation)
    return LANGUAGES.get(match[1], match[1]) if match else "English"


def parse_gkg(row, drop_url):
    if len(row) < 27 or row[2] != "1":
        return None
    locations = []
    for location in row[10].split(";")[:200]:
        fields = location.split("#")
        if len(fields) >= 9:
            try:
                lat, lon = float(fields[5]), float(fields[6])
                if -90 <= lat <= 90 and -180 <= lon <= 180:
                    locations.append(
                        {
                            "name": fields[1][:160],
                            "country_code": FIPS_ISO.get(fields[2], ""),
                            "fips": fields[2],
                            "lat": lat,
                            "lon": lon,
                            "geo_type": fields[0],
                            "offset": int(fields[8]) if fields[8].isdigit() else None,
                        }
                    )
            except ValueError:
                continue
    amounts = []
    for item in row[24].split(";")[:20]:
        fields = item.rsplit(",", 1)[0].split(",", 1)
        if len(fields) == 2 and number(fields[0]) is not None:
            amounts.append({"value": number(fields[0]), "object": clean(fields[1], 100)})
    quotes = []
    for item in row[22].split("#")[:8]:
        fields = item.split("|", 3)
        if len(fields) == 4 and fields[3].strip():
            quotes.append({"verb": clean(fields[2], 40), "text": clean(fields[3], 400)})
    result = {
        "seen_at": timestamp(row[1]).isoformat(),
        "language": source_language(row[25]),
        "themes": list(dict.fromkeys(x.split(",")[0] for x in row[8].split(";") if x))[:160],
        "persons": list(dict.fromkeys(x.rsplit(",", 1)[0] for x in row[12].split(";") if x))[:15],
        "organizations": list(dict.fromkeys(x.rsplit(",", 1)[0] for x in row[14].split(";") if x))[:15],
        "locations": locations,
        "tone": row[15][:120],
        "amounts": amounts,
        "quotations": quotes,
        "all_names": list(dict.fromkeys(clean(x.rsplit(",", 1)[0], 100) for x in row[23].split(";") if x))[
            :40
        ],
        "gkg_counts": [clean(x, 400) for x in row[6].split(";") if x][:15],
        "date_mentions": [clean(x, 100) for x in row[16].split(";") if x][:20],
        "gcam": {
            part.split(":", 1)[0]: number(part.split(":", 1)[1])
            for part in row[17].split(",")
            if ":" in part and part.split(":", 1)[0] in {"wc", "c8.3", "c6.6"}
        },
        "provenance_gkg": {
            "dataset": "GKG 2.1",
            "drop": drop_url,
            "record_id": row[0],
            "fields": [
                "V2Themes",
                "V2Persons",
                "V2Organizations",
                "V2Locations",
                "V2Tone",
                "V2TranslationInfo",
                "V2ExtrasXML.PAGE_TITLE",
                "V2.1Amounts",
                "V2.1Quotations",
                "V2.1AllNames",
                "V2GCAM.wc",
                "V2GCAM.c8.3",
                "V2GCAM.c6.6",
            ],
        },
    }
    title = extra_field(row[26], "PAGE_TITLE")
    if title:
        result["title"] = title
    author = extra_field(row[26], "PAGE_AUTHORS", 200)
    if author:
        result["author"] = author
    published = extra_field(row[26], "PAGE_PRECISEPUBTIMESTAMP", 40)
    if re.fullmatch(r"\d{14}", published):
        try:
            result["published_at"] = timestamp(published).isoformat()
        except ValueError:
            pass
    return canonical_url(row[4]), result


def parse_gal(record, drop_url):
    # GAL.date may be publisher time; it is NOT used as first-seen evidence.
    result = {
        "title": clean(record.get("title", "")),
        "description": clean(record.get("desc", ""), 600),
        "outlet": str(record.get("outletName", record.get("domain", "")))[:200],
        "author": clean(record.get("author", ""), 200),
        "provenance_gal": {
            "dataset": "GAL",
            "drop": drop_url,
            "fields": ["title", "desc", "outletName", "author", "lang"],
        },
    }
    if record.get("lang"):
        result["language"] = normalize_language(record["lang"])
    return canonical_url(record["url"]), {k: v for k, v in result.items() if v}


def parse_gemg(record, drop_url):
    tags = {
        str(t.get("key", "")).lower(): clean(t.get("value"), 600)
        for t in record.get("metatags", [])[:200]
        if isinstance(t, dict)
    }
    state = {
        "title": clean(record.get("title") or tags.get("og:title")),
        "description": tags.get("og:description") or tags.get("description", ""),
        "author": clean(tags.get("author"), 200),
        "outlet": clean(tags.get("og:site_name"), 200),
        "language": normalize_language(record["lang"]) if record.get("lang") else "",
        "provenance_gemg": {
            "dataset": "GEMG",
            "drop": drop_url,
            "fields": ["title", "lang", "metatags.description", "metatags.author", "metatags.og:site_name"],
        },
    }
    if record.get("date"):
        state["seen_at"] = timestamp(record["date"]).isoformat()
    return canonical_url(record["url"]), {k: v for k, v in state.items() if v}

