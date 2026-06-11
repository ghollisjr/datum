"""Admin panels for datum.

Each panel module exposes:
    get_data(cursor, driver, args) -> dict with keys:
        panel    - panel name (str)
        headers  - column headers (list of str)
        rows     - data rows (list of lists)
        row_id   - column index used as row identifier (int), or None
        actions  - available actions (list of dicts: {key, label, command})
        info     - optional status/info text (str or None)
"""
