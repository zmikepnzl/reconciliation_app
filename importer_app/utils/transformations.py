import pandas as pd
import logging
from db_client import DBClient

def apply_transformation(value, transformation):
    """
    Applies a data type transformation to a given value, combining logic
    from both previous versions for robustness.
    """
    if pd.isna(value) or value is None:
        return None # Return None to represent NULL in the database

    t = (transformation or 'none').replace(" ", "").lower()

    if t == "none":
        return value
    if t == "totext":
        return str(value)
    if t == "tointeger":
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
    if t == "todecimal":
        try:
            # Use rounding to ensure consistent decimal places
            return round(float(value), 2)
        except (ValueError, TypeError):
            return None
    if t in ["todate", "todate(iso)"]:
        try:
            # CORRECTED: Use dayfirst=True to correctly parse DD/MM/YYYY dates
            dt = pd.to_datetime(value, errors="coerce", dayfirst=True)
            return dt.strftime("%Y-%m-%d") if pd.notna(dt) else None
        except Exception:
            return None
    if t == 'tonegative':
        try:
            return -abs(float(value))
        except (ValueError, TypeError):
            return None
            
    return value

def get_link_row_id(client: DBClient, table_name: str, field_name: str, field_value, supplier_id: int = None):
    """
    Finds the ID of a row in a linked table using the DBClient's helper methods.
    If the table is 'supplier_account' and the row doesn't exist, it creates it.
    """
    if not all([table_name, field_name, field_value is not None, str(field_value).strip()]):
        return None

    row_id = client.get_row_id(table_name, field_name, str(field_value).strip())

    # Special handling to auto-create supplier accounts if they don't exist
    if not row_id and table_name == 'supplier_account' and supplier_id:
        logging.info(f"Creating new supplier_account for '{field_value}' linked to supplier ID {supplier_id}.")
        new_account_data = {
            "supplier_id": supplier_id,
            "account_number": str(field_value).strip()
        }
        new_account = client.create_row("supplier_account", new_account_data)
        if new_account:
            row_id = new_account['id']
            logging.info(f"Successfully created supplier_account with ID {row_id}.")
        else:
            logging.error(f"Failed to create new supplier_account for '{field_value}'.")

    return row_id

def get_val(ctx, *keys):
    """
    Return the first non-empty value found by trying all the given keys/case-variants.
    """
    for k in keys:
        # Direct key match
        if k in ctx and pd.notna(ctx[k]) and ctx[k]:
            return str(ctx[k])
        # Case-insensitive match
        kl = k.lower()
        for kc in ctx.keys():
            if kc.lower() == kl and pd.notna(ctx[kc]) and ctx[kc]:
                return str(ctx[kc])
    return ""
