import re
import unicodedata

STREET_SUFFIXES = {
    "STREET": "ST",
    "ST": "ST",
    "AVENUE": "AVE",
    "AV": "AVE",
    "ROAD": "RD",
    "RD": "RD",
    "BOULEVARD": "BLVD",
    "BLVD": "BLVD",
    "DRIVE": "DR",
    "DR": "DR",
    "LANE": "LN",
    "LN": "LN",
    "COURT": "CT",
    "CT": "CT",
    "CIRCLE": "CIR",
    "CIR": "CIR",
    "HIGHWAY": "HWY",
    "HWY": "HWY",
    "PARKWAY": "PKWY",
    "PKWY": "PKWY",
    "PLACE": "PL",
    "PL": "PL",
    "TERRACE": "TER",
    "TER": "TER",
    "TRAIL": "TRL",
    "TRL": "TRL",
    "WAY": "WAY",
}

DIRECTIONALS = {
    "NORTH": "N",
    "SOUTH": "S",
    "EAST": "E",
    "WEST": "W",
    "NORTHEAST": "NE",
    "NORTHWEST": "NW",
    "SOUTHEAST": "SE",
    "SOUTHWEST": "SW",
}

def normalize_address(address):
    if not address:
        return ""

    # unicode normalization
    address = unicodedata.normalize("NFKC", address)
    
    # uppercase
    address = address.upper()

    # remove punctuation except for apartment/unit separators if wanted
    address = re.sub(r"[.,]", "", address)

    # normalize whitespace
    address = re.sub(r"\s+", " ", address).strip()

    # normalize directional words
    words = address.split()

    normalized_words = []

    for word in words:
        word = DIRECTIONALS.get(word, word)
        word = STREET_SUFFIXES.get(word, word)
        normalized_words.append(word)
    
    return " ".join(normalized_words)