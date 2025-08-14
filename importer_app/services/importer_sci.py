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

def import_sci_csv(client, filepath: str) -> int:
    """
    Imports a Service Catalog Items CSV file into the PostgreSQL database.
    It performs an "upsert" operation.
    """
    logging.debug(f"Starting SCI import for file: {filepath}")
    try:
        df = pd.read_csv(filepath, dtype=str).fillna('')
        updated_count = 0
        
        customer_name = os.path.basename(filepath).split('-')[0].strip()
        logging.debug(f"Extracted customer name from filename: '{customer_name}'")
        
        customer_id = client.get_row_id("customers", "name", customer_name)
        
        if not customer_id:
            logging.warning(f"Customer '{customer_name}' not found. Creating a new customer.")
            new_customer = client.create_row("customers", {"name": customer_name})
            customer_id = new_customer.get('id')
            
        logging.debug(f"Using customer ID: {customer_id}")

        for _, row in df.iterrows():
            zeus_id_val = row.get('id') or row.get('Zeus ID')
            if not zeus_id_val:
                continue

            sci_data = {
                "zeus_id": _nan_to_null(zeus_id_val),
                "service_line": _nan_to_null(row.get('service_line')),
                "master_service_catalogue_item": _nan_to_null(row.get('master_service_catalogue_item')),
                "netsuite_ru_item_code": _nan_to_null(row.get('netsuite_ru_item_code')),
                "unit": _nan_to_null(row.get('unit')),
                "service_billing_method": _nan_to_null(row.get('service_billing_method')),
                "billing_name": _nan_to_null(row.get('billing_name')),
                "host_types": _nan_to_null(row.get('host_types')),
                "service_description": _nan_to_null(row.get('service_description')),
                "billable": str(row.get('billable', 'False')).lower() in ['true', '1', 't', 'y', 'yes'],
                "include_in_billing_run": str(row.get('include_in_billing_run', 'False')).lower() in ['true', '1', 't', 'y', 'yes'],
                "contract_number": _nan_to_null(row.get('contract_number')),
                "service_key": _nan_to_null(row.get('service_key')),
                "datacom_cost_price": _nan_to_null(pd.to_numeric(row.get('datacom_cost_price'), errors='coerce')),
                "customer_standard_price": _nan_to_null(pd.to_numeric(row.get('customer_standard_price'), errors='coerce')),
                "adjustment_percentage": _nan_to_null(pd.to_numeric(row.get('adjustment_percentage'), errors='coerce')),
                "customer_sell_price": _nan_to_null(pd.to_numeric(row.get('customer_sell_price'), errors='coerce')),
                "additional_info": _nan_to_null(row.get('additional_info')),
                "billable_quantity": _nan_to_null(pd.to_numeric(row.get('billable_quantity'), errors='coerce')),
                "pipeline_quantity": _nan_to_null(pd.to_numeric(row.get('pipeline_quantity'), errors='coerce')),
                "total_bill_price": _nan_to_null(pd.to_numeric(row.get('total_bill_price'), errors='coerce')),
                "customer_id": customer_id
            }

            sci_data = {k: v for k, v in sci_data.items() if v is not None}
            
            existing_id = client.get_row_id("service_catalog_items", "zeus_id", sci_data.get("zeus_id"))
            
            if existing_id:
                client.update_row("service_catalog_items", existing_id, sci_data)
            else:
                client.create_row("service_catalog_items", sci_data)
            
            updated_count += 1
            
        logging.info(f"Successfully imported or updated {updated_count} SCI rows from {filepath}.")
        return updated_count

    except Exception as e:
        logging.error(f"Failed to import SCI file {filepath}: {e}")
        raise