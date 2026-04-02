"""
Unified category system.
HanseaticBank categories are used as the base; comdirect transactions
are mapped to the same set via keyword rules.
"""

CATEGORIES = [
    "Einkommen",
    "Lebensmittel",
    "Restaurant & Cafe",
    "Mobilität",
    "Einkaufen",
    "Kleidung",
    "Kinder",
    "Hobby",
    "Gesundheit",
    "Unterhaltung",
    "Finanzen & Versicherung",
    "Wohnen & Nebenkosten",
    "Sparen & Investieren",
    "Überweisung",
    "Sonstiges",
]

# Maps HanseaticBank merchantData.category → unified category
HANSEATICBANK_CATEGORY_MAP = {
    "Eating Out":           "Restaurant & Cafe",
    "Groceries":            "Lebensmittel",
    "Mobility":             "Mobilität",
    "Shopping":             "Einkaufen",
    "Health":               "Gesundheit",
    "Entertainment":        "Unterhaltung",
    "Finance & Insurance":  "Finanzen & Versicherung",
    "General":              "Sonstiges",
}

# Keyword rules for comdirect CSV (applied to description/merchant in order)
# Each entry: (pattern_lower, category)
COMDIRECT_KEYWORD_RULES = [
    # Einkommen
    ("lohn/gehalt",         "Einkommen"),
    ("gehalt",              "Einkommen"),
    ("lohn",                "Einkommen"),
    ("kindergeld",          "Einkommen"),
    ("familienkasse",       "Einkommen"),
    ("steuererstattung",    "Einkommen"),
    ("anlegerauszahlung",   "Einkommen"),

    # Sparen & Investieren
    ("scalable",            "Sparen & Investieren"),
    ("tagesgeld",           "Sparen & Investieren"),
    ("sparplan",            "Sparen & Investieren"),
    ("depot",               "Sparen & Investieren"),
    ("invest",              "Sparen & Investieren"),

    # Wohnen & Nebenkosten
    ("miete",               "Wohnen & Nebenkosten"),
    ("strom",               "Wohnen & Nebenkosten"),
    ("enbw",                "Wohnen & Nebenkosten"),
    ("yello",               "Wohnen & Nebenkosten"),
    ("gas ",                "Wohnen & Nebenkosten"),
    ("wasser",              "Wohnen & Nebenkosten"),
    ("nebenkosten",         "Wohnen & Nebenkosten"),
    ("hausgeld",            "Wohnen & Nebenkosten"),

    # Mobilität
    ("tankstelle",          "Mobilität"),
    ("tanken",              "Mobilität"),
    ("aral",                "Mobilität"),
    ("shell",               "Mobilität"),
    ("esso",                "Mobilität"),
    ("baywa",               "Mobilität"),
    ("easypark",            "Mobilität"),
    ("paybyphone",          "Mobilität"),
    ("handyparken",         "Mobilität"),
    ("parken",              "Mobilität"),
    ("deutsche bahn",       "Mobilität"),
    ("db ",                 "Mobilität"),
    ("mvv",                 "Mobilität"),
    ("öpnv",                "Mobilität"),

    # Lebensmittel
    ("rewe",                "Lebensmittel"),
    ("edeka",               "Lebensmittel"),
    ("aldi",                "Lebensmittel"),
    ("lidl",                "Lebensmittel"),
    ("penny",               "Lebensmittel"),
    ("netto",               "Lebensmittel"),
    ("kaufland",            "Lebensmittel"),
    ("norma",               "Lebensmittel"),
    ("bäckerei",            "Lebensmittel"),
    ("brothaus",            "Lebensmittel"),
    ("backhaus",            "Lebensmittel"),

    # Restaurant & Cafe
    ("restaurant",          "Restaurant & Cafe"),
    ("espressobar",         "Restaurant & Cafe"),
    ("café",                "Restaurant & Cafe"),
    ("cafe",                "Restaurant & Cafe"),
    ("gasthaus",            "Restaurant & Cafe"),
    ("bistro",              "Restaurant & Cafe"),
    ("pizzeria",            "Restaurant & Cafe"),
    ("sushi",               "Restaurant & Cafe"),
    ("mcdonalds",           "Restaurant & Cafe"),
    ("burger king",         "Restaurant & Cafe"),

    # Gesundheit
    ("apotheke",            "Gesundheit"),
    ("rossmann",            "Gesundheit"),
    ("dm ",                 "Gesundheit"),
    ("arzt",                "Gesundheit"),
    ("krankenversicherung", "Gesundheit"),
    ("ergo kranken",        "Gesundheit"),
    ("musikschule",         "Gesundheit"),  # child activities

    # Unterhaltung
    ("netflix",             "Unterhaltung"),
    ("spotify",             "Unterhaltung"),
    ("apple.com/bill",      "Unterhaltung"),
    ("amazon prime",        "Unterhaltung"),
    ("audible",             "Unterhaltung"),
    ("kino",                "Unterhaltung"),
    ("theater",             "Unterhaltung"),
    ("disney",              "Unterhaltung"),

    # Finanzen & Versicherung
    ("allianz",             "Finanzen & Versicherung"),
    ("versicherung",        "Finanzen & Versicherung"),
    ("hanseatic bank",      "Finanzen & Versicherung"),
    ("kartenabrechnung",    "Finanzen & Versicherung"),
    ("visa",                "Finanzen & Versicherung"),
    ("kontoabschluss",      "Finanzen & Versicherung"),
    ("entgelt",             "Finanzen & Versicherung"),
    ("kontoführungsentgelt","Finanzen & Versicherung"),

    # Einkaufen
    ("amazon",              "Einkaufen"),
    ("h&m",                 "Einkaufen"),
    ("zara",                "Einkaufen"),
    ("obi ",                "Einkaufen"),
    ("bauhaus",             "Einkaufen"),
    ("ikea",                "Einkaufen"),
    ("zalando",             "Einkaufen"),
    ("ebay",                "Einkaufen"),
    ("aliexpress",          "Einkaufen"),
    ("riverty",             "Einkaufen"),
    ("paypal",              "Einkaufen"),

    # Telekommunikation → Wohnen
    ("telekom",             "Wohnen & Nebenkosten"),
    ("congstar",            "Wohnen & Nebenkosten"),
    ("vodafone",            "Wohnen & Nebenkosten"),
    ("o2",                  "Wohnen & Nebenkosten"),

    # Überweisung (catch-all for internal transfers)
    ("übertrag",            "Überweisung"),
    ("uebertrag",           "Überweisung"),
]


def categorize_from_comdirect(vorgang: str, description: str) -> str:
    text = (vorgang + " " + description).lower()
    for pattern, category in COMDIRECT_KEYWORD_RULES:
        if pattern in text:
            return category
    return "Sonstiges"


def categorize_from_hanseatic(hb_category: str, hb_subcategories: list) -> str:
    if hb_category in HANSEATICBANK_CATEGORY_MAP:
        return HANSEATICBANK_CATEGORY_MAP[hb_category]
    return "Sonstiges"
