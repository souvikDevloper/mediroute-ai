"""Small offline Ghana geocoding helper for demo maps.

The official dataset often provides city names but not coordinates. We avoid any
external geocoding dependency during the hackathon and use a conservative offline
lookup for common Ghanaian cities/regions. Unknown locations fall back to Accra
only at the map layer, while the dashboard still preserves the original city.
"""

from __future__ import annotations

from .text_utils import normalize

GHANA_COORDS: dict[str, tuple[float, float]] = {
    "accra": (5.6037, -0.1870),
    "tema": (5.6698, -0.0166),
    "kumasi": (6.6666, -1.6163),
    "tamale": (9.4034, -0.8424),
    "takoradi": (4.8845, -1.7554),
    "sekondi-takoradi": (4.9340, -1.7137),
    "cape coast": (5.1053, -1.2466),
    "sunyani": (7.3349, -2.3123),
    "ho": (6.6008, 0.4713),
    "koforidua": (6.0941, -0.2591),
    "bolgatanga": (10.7856, -0.8514),
    "wa": (10.0607, -2.5019),
    "techiman": (7.5842, -1.9381),
    "yendi": (9.4427, -0.0099),
    "navrongo": (10.8956, -1.0921),
    "bawku": (11.0616, -0.2417),
    "ashaiman": (5.6920, -0.0330),
    "tarkwa": (5.3018, -1.9930),
    "berekum": (7.4534, -2.5840),
    "obuasi": (6.2000, -1.6833),
    "east legon": (5.6500, -0.1500),
    "osu": (5.5560, -0.1820),
    "weija": (5.5750, -0.3350),
    "battor": (6.0560, 0.3690),
    "atebubu": (7.7560, -0.9860),
    "madina": (5.6820, -0.1660),
    "dansoman": (5.5400, -0.2680),
    "north kaneshie": (5.5810, -0.2190),
    "ablekuma": (5.5960, -0.3030),
    "kasoa": (5.5345, -0.4168),
    "hohoe": (7.1518, 0.4736),
    "nsawam": (5.8089, -0.3503),
    "winneba": (5.3511, -0.6231),
    "akosombo": (6.2958, 0.0608),
    "sogakope": (5.9992, 0.5943),
    "dome": (5.6500, -0.2400),
    "adenta": (5.7045, -0.1668),
}

REGION_COORDS: dict[str, tuple[float, float]] = {
    "greater accra": (5.6037, -0.1870),
    "ashanti": (6.6666, -1.6163),
    "northern": (9.4034, -0.8424),
    "western": (4.8845, -1.7554),
    "central": (5.1053, -1.2466),
    "bono": (7.3349, -2.3123),
    "volta": (6.6008, 0.4713),
    "eastern": (6.0941, -0.2591),
    "upper east": (10.7856, -0.8514),
    "upper west": (10.0607, -2.5019),
    "ghana": (7.9465, -1.0232),
}


def infer_lat_lon(city: object = "", region: object = "", name: object = "") -> tuple[float | None, float | None]:
    candidates = [normalize(city), normalize(region), normalize(name)]
    for c in candidates:
        if not c:
            continue
        if c in GHANA_COORDS:
            return GHANA_COORDS[c]
        for key, val in GHANA_COORDS.items():
            if key and key in c:
                return val
        if c in REGION_COORDS:
            return REGION_COORDS[c]
        for key, val in REGION_COORDS.items():
            if key and key in c:
                return val
    return None, None

GHANA_REGIONS = {
    "greater accra": "Greater Accra",
    "greater accra region": "Greater Accra",
    "accra": "Greater Accra",
    "ashanti": "Ashanti",
    "ashanti region": "Ashanti",
    "central": "Central",
    "central region": "Central",
    "western": "Western",
    "western region": "Western",
    "western north": "Western North",
    "western north region": "Western North",
    "northern": "Northern",
    "northern region": "Northern",
    "volta": "Volta",
    "volta region": "Volta",
    "eastern": "Eastern",
    "eastern region": "Eastern",
    "bono": "Bono",
    "bono region": "Bono",
    "brong ahafo": "Bono",
    "brong ahafo region": "Bono",
    "bono east": "Bono East",
    "bono east region": "Bono East",
    "ahafo": "Ahafo",
    "ahafo region": "Ahafo",
    "upper east": "Upper East",
    "upper east region": "Upper East",
    "upper west": "Upper West",
    "upper west region": "Upper West",
    "oti": "Oti",
    "oti region": "Oti",
    "savannah": "Savannah",
    "savannah region": "Savannah",
    "north east": "North East",
    "north east region": "North East",
}

CITY_TO_REGION = {
    # Greater Accra
    "accra": "Greater Accra", "tema": "Greater Accra", "ashaiman": "Greater Accra", "east legon": "Greater Accra",
    "osu": "Greater Accra", "weija": "Greater Accra", "madina": "Greater Accra", "dansoman": "Greater Accra",
    "north kaneshie": "Greater Accra", "ablekuma": "Greater Accra", "kasoa": "Central", "dome": "Greater Accra",
    "adenta": "Greater Accra", "adenta municipality": "Greater Accra", "tesano": "Greater Accra", "nungua": "Greater Accra",
    "oyarifa": "Greater Accra", "dodowa": "Greater Accra", "cantonments": "Greater Accra", "agbogbloshie": "Greater Accra",
    "darkuman-nyamekye": "Greater Accra", "greater accra": "Greater Accra",
    # Ashanti
    "kumasi": "Ashanti", "obuasi": "Ashanti", "kwadaso": "Ashanti", "mampong": "Ashanti", "ejisu": "Ashanti",
    "tikrom": "Ashanti", "kuntanase": "Ashanti", "dompoase": "Ashanti", "tepa": "Ashanti",
    # Western / Western North
    "takoradi": "Western", "sekondi": "Western", "tarkwa": "Western", "agona nkwanta": "Western", "western": "Western",
    "bibiani": "Western North", "sefwi bekwai": "Western North", "sefwi wiawso": "Western North", "enchi": "Western North",
    # Central
    "cape coast": "Central", "agona swedru": "Central", "mankessim": "Central", "winneba": "Central", "breman asikuma": "Central",
    # Northern / North East / Savannah
    "tamale": "Northern", "yendi": "Northern", "walewale": "North East", "kpandai": "Northern", "gwo": "Northern",
    # Bono / Bono East / Ahafo
    "sunyani": "Bono", "berekum": "Bono", "dormaa ahenkro": "Bono", "wenchi": "Bono", "duayaw nkwanta": "Ahafo",
    "techiman": "Bono East", "atebubu": "Bono East", "kintampo": "Bono East", "bechem": "Ahafo",
    # Volta / Oti
    "ho": "Volta", "hohoe": "Volta", "kpando": "Volta", "sogakope": "Volta", "keta": "Volta", "akatsi": "Volta",
    "adidome": "Volta", "dzodze": "Volta", "battor": "Volta", "aflao": "Volta", "nkwanta": "Oti", "worawora": "Oti",
    # Eastern
    "koforidua": "Eastern", "akosombo": "Eastern", "nkawkaw": "Eastern", "nsawam": "Eastern", "somanya": "Eastern", "akwatia": "Eastern",
    # Upper regions
    "bolgatanga": "Upper East", "bawku": "Upper East", "navrongo": "Upper East", "wa": "Upper West",
}


def canonical_region(raw_region: object = "", city: object = "") -> str:
    r = normalize(raw_region)
    c = normalize(city)
    if r in GHANA_REGIONS:
        return GHANA_REGIONS[r]
    if c in CITY_TO_REGION:
        return CITY_TO_REGION[c]
    # Some source rows put city/district names in the state field.
    if r in CITY_TO_REGION:
        return CITY_TO_REGION[r]
    # Do not create hundreds of pseudo-regions from facility-local towns.
    # Keep the town in the separate `city` column and group unmapped locations.
    return "Other / Unmapped"
