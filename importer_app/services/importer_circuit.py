import pandas as pd
import logging
import os
import numpy as np

from db_client import DBClient
from utils.transformations import apply_transformation, get_link_row_id

def _nan_to_null(v):
    if v is None or (isinstance(v, (float, np.floating)) and np.isnan(v)) or v == '':
        return None
    if isinstance(v, (np.float64, np.int64)):
        return v.item()
    return v

def safe_parse_date(date_str):
    if not date_str or pd.isna(date_str) or date_str == '':
        return None
    try:
        # Assuming the date format is flexible, let pandas infer
        dt = pd.to_datetime(date_str, errors='coerce')
        if pd.notna(dt):
            return dt.date()
    except Exception:
        pass
    return None

def import_circuit_csv(client, filepath: str) -> int:
    """
    Imports a CMDB Circuits CSV file into the PostgreSQL database.
    Performs an "upsert" operation based on the CMDB 'zeus_id'.
    """
    logging.debug(f"Starting Circuit import for file: {filepath}")
    try:
        df = pd.read_csv(filepath, dtype=str).fillna('')
        updated_count = 0
        
        customer_name = os.path.basename(filepath).split('-')[0].strip()
        customer_id = client.get_row_id("customers", "name", customer_name)
        if not customer_id:
            logging.warning(f"Customer '{customer_name}' not found in DB. Creating new customer.")
            new_customer = client.create_row("customers", {"name": customer_name})
            customer_id = new_customer.get('id')
        
        for _, row in df.iterrows():
            zeus_id_val = row.get('id')
            if not zeus_id_val or pd.isna(zeus_id_val):
                logging.warning("Skipping row with missing CMDB 'id'.")
                continue

            circuit_data = {
                "zeus_id": _nan_to_null(zeus_id_val),
                "circuit_id": _nan_to_null(row.get('name') or row.get('Circuit ID')),
                "circuit_name": _nan_to_null(row.get('description')),
                "status": _nan_to_null(row.get('state')),
                "site_name": _nan_to_null(row.get('site')),
                "site_code": _nan_to_null(row.get('site_code')),
                "telco": _nan_to_null(row.get('telco')),
                "priority": _nan_to_null(row.get('priority')),
                "circuit_type": _nan_to_null(row.get('circuit_type')),
                "circuit_service_type": _nan_to_null(row.get('circuit_service_type')),
                "circuit_termination_type": _nan_to_null(row.get('circuit_termination_type')),
                "access": _nan_to_null(row.get('access')),
                "cir": _nan_to_null(row.get('cir')),
                "pir": _nan_to_null(row.get('pir')),
                "vlan": _nan_to_null(row.get('vlan')),
                "pe_host": _nan_to_null(row.get('pe_host')),
                "ip_pe": _nan_to_null(row.get('ip_pe')),
                "ce_host": _nan_to_null(row.get('ce_host')),
                "host": _nan_to_null(row.get('host')),
                "ip_router": _nan_to_null(row.get('ip_router')),
                "interface": _nan_to_null(row.get('interface')),
                "linknet": _nan_to_null(row.get('linknet')),
                "qosprofile": _nan_to_null(row.get('qosprofile')),
                "notes": _nan_to_null(row.get('notes')),
                "netops_sensor_id": _nan_to_null(row.get('netops_sensor_id')),
                "netflow": str(row.get('netflow', 'False')).lower() in ['true', '1', 't', 'y', 'yes'],
                "contract_start": safe_parse_date(row.get('contract_start')),
                "contract_end": safe_parse_date(row.get('contract_end')),
                "exchange": _nan_to_null(row.get('exchange')),
                "region": _nan_to_null(row.get('region')),
                "activated": safe_parse_date(row.get('activated')),
                "decommissioned": safe_parse_date(row.get('decommissioned')),
                "vendor_billing_end_date": safe_parse_date(row.get('vendor_billing_end_date')),
                "service_end_date": safe_parse_date(row.get('service_end_date')),
                "datacom_last_billing_run_date": safe_parse_date(row.get('datacom_last_billing_run_date')),
                "datacom_billing_code": _nan_to_null(row.get('datacom_billing_code')),
                "vendor_account_number": _nan_to_null(row.get('vendor_account_number')),
                "service_catalogue_item_imported": _nan_to_null(row.get('service_catalogue_item')),
                "customer_id": customer_id
            }
            
            # Clean data: remove keys with None or empty string values
            circuit_data = {k: v for k, v in circuit_data.items() if v is not None and v != ''}

            logging.debug(f"Checking for existing circuit with CMDB 'id': '{zeus_id_val}'")
            existing_id = client.get_row_id("cmdb_circuits", "zeus_id", zeus_id_val)
            
            if existing_id:
                logging.debug(f"Found existing circuit for 'zeus_id': '{zeus_id_val}' with ID: {existing_id}. Updating record.")
                client.update_row("cmdb_circuits", existing_id, circuit_data)
            else:
                logging.debug(f"No existing circuit found for 'zeus_id': '{zeus_id_val}'. Creating a new record.")
                client.create_row("cmdb_circuits", circuit_data)
            
            updated_count += 1
        
        logging.info(f"Successfully imported or updated {updated_count} Circuit rows from {filepath}.")
        return updated_count

    except Exception as e:
        logging.error(f"Failed to import Circuit file {filepath}: {e}")
        raise