"""Villages to scrape.

Each entry:
  name      — Georgian address string passed to the NAPR search.
  date_from — optional ISO 'YYYY-MM-DD'; only records on/after this date.
              Omit to scrape full history.
"""

VILLAGES = [
    {"name": "წინამძღვრიანთკარი"},
    {"name": "საგურამო", "date_from": "2026-01-01"},
]
