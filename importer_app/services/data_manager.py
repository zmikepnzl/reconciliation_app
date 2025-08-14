# services/data_manager.py

import json
import logging
from db_client import DBClient
from psycopg2.errors import UndefinedTable
import traceback
from typing import Tuple, List

def check_schema_status(client: DBClient, destination_schema: dict) -> Tuple[str, List[str]]:
    """
    Checks if the database schema is complete by comparing it to the
    application's DESTINATION_SCHEMA. Returns a tuple of status and a list of errors.
    """
    errors = []
    
    for role, details in destination_schema.items():
        table_name = details.get('table')
        if not table_name:
            continue
            
        expected_columns = {field.replace(' ', '_').lower() for field in details['fields'].keys() if field.replace(' ', '_').lower() not in ['id']}
        
        try:
            db_columns_raw = client._execute_query(
                f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = '{table_name.lower()}';
                """,
                fetch='all'
            )
            db_columns = {r['column_name'] for r in db_columns_raw}

            # Check for missing columns
            missing_columns = expected_columns - db_columns
            if missing_columns:
                errors.append(f"Table '{table_name}' is missing the following columns: {', '.join(missing_columns)}")

            # Check for extra columns (ignore id and foreign keys for this check)
            extra_columns = db_columns - expected_columns
            ignored_columns = {'id', 'supplier_id', 'invoice_header_id', 'item_id', 'mapping_name_id'}
            extra_columns = extra_columns - ignored_columns
            if extra_columns:
                errors.append(f"Table '{table_name}' has unexpected columns: {', '.join(extra_columns)}")

        except UndefinedTable:
            errors.append(f"Table '{table_name}' does not exist.")
        except Exception as e:
            errors.append(f"Error checking schema for table '{table_name}': {e}")
            logging.error(f"Error checking schema: {traceback.format_exc()}")
            
    if errors:
        return "error", errors
    
    return "ok", errors

def export_db_schema(client: DBClient, filepath: str) -> None:
    """Exports the current database schema to a JSON file."""
    try:
        schema_query = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'public'
        ORDER BY table_name, ordinal_position;
        """
        db_schema_raw = client._execute_query(schema_query, fetch='all')
        
        db_schema = {}
        for row in db_schema_raw:
            table = row['table_name']
            if table not in db_schema:
                db_schema[table] = []
            db_schema[table].append(row)
        
        with open(filepath, 'w') as f:
            json.dump(db_schema, f, indent=4)
        
        logging.info(f"Successfully exported database schema to {filepath}")
    except Exception as e:
        logging.error(f"Error during schema export: {e}")
        raise

def export_mapping_data(client: DBClient, filepath: str) -> None:
    """Exports all suppliers, mappings, and mapping lines to a JSON file."""
    try:
        suppliers_raw = client.get_rows("suppliers")
        mappings_raw = client.get_rows("import_mappings")
        mapping_lines_raw = client.get_rows("import_mapping_lines")

        suppliers = [dict(s) for s in suppliers_raw]
        mappings = [dict(m) for m in mappings_raw]
        mapping_lines = [dict(ml) for ml in mapping_lines_raw]

        data = {
            "suppliers": suppliers,
            "import_mappings": mappings,
            "import_mapping_lines": mapping_lines
        }

        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
            
        logging.info(f"Successfully exported all mapping data to {filepath}")
    except Exception as e:
        logging.error(f"Error during data export: {e}")
        raise

def import_mapping_data(client: DBClient, filepath: str) -> None:
    """Imports suppliers, mappings, and mapping lines from a JSON file."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)

        if not data:
            logging.warning("Import file is empty.")
            return

        old_to_new_supplier_ids = {}
        old_to_new_mapping_ids = {}

        for s in data.get("suppliers", []):
            if not isinstance(s, dict):
                logging.warning(f"Skipping malformed supplier entry: {s}")
                continue
            old_id = s.pop('id')
            new_supplier = client.create_row("suppliers", s)
            old_to_new_supplier_ids[old_id] = new_supplier['id']

        for m in data.get("import_mappings", []):
            if not isinstance(m, dict):
                logging.warning(f"Skipping malformed mapping entry: {m}")
                continue
            old_id = m.pop('id')
            old_supplier_id = m.pop('supplier_id')
            m['supplier_id'] = old_to_new_supplier_ids.get(old_supplier_id)
            if m['supplier_id']:
                new_mapping = client.create_row("import_mappings", m)
                old_to_new_mapping_ids[old_id] = new_mapping['id']
        
        for ml in data.get("import_mapping_lines", []):
            if not isinstance(ml, dict):
                logging.warning(f"Skipping malformed mapping line entry: {ml}")
                continue
            old_id = ml.pop('id')
            old_mapping_id = ml.pop('mapping_name_id')
            ml['mapping_name_id'] = old_to_new_mapping_ids.get(old_mapping_id)
            if ml['mapping_name_id']:
                client.create_row("import_mapping_lines", ml)

        logging.info("Successfully imported all mapping data.")

    except Exception as e:
        logging.error(f"Error during data import: {e}")
        raise