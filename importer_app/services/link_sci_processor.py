import logging
from db_client import DBClient

def process_sci_linking(client) -> int:
    """
    Links circuits to service catalog items in bulk using an efficient SQL query.
    This is much faster than fetching all data into memory.
    """
    logging.info("Starting SCI to Circuit linking process.")
    
    # NEW DEBUG STEP: Log potential matches before running the update
    select_sql = """
        SELECT
            c.id as circuit_id,
            c.service_catalogue_item_imported,
            sci.id as sci_id,
            sci.billing_name
        FROM cmdb_circuits c
        JOIN service_catalog_items sci ON c.service_catalogue_item_imported = sci.billing_name
        WHERE c.customer_id = sci.customer_id
          AND c.service_catalogue_item_linked_id IS NULL;
    """
    
    # The client parameter is already passed from a 'with' block,
    # so we can use its cursor directly.
    try:
        # Log potential matches for debugging
        client.cursor.execute(select_sql)
        potential_matches = client.cursor.fetchall()
        
        logging.info(f"Found {len(potential_matches)} potential matches for linking. Details of first 5:")
        for match in potential_matches[:5]:
            logging.info(match)
        
        # Now, proceed with the actual UPDATE query
        sql = """
            UPDATE cmdb_circuits
            SET service_catalogue_item_linked_id = sci.id
            FROM service_catalog_items sci
            WHERE cmdb_circuits.service_catalogue_item_imported = sci.billing_name
              AND cmdb_circuits.customer_id = sci.customer_id
              AND cmdb_circuits.service_catalogue_item_linked_id IS NULL;
        """
        
        client.cursor.execute(sql)
        linked_count = client.cursor.rowcount
        # The calling 'with' block will handle the commit/rollback.
        logging.info(f"Successfully linked {linked_count} new circuits to SCIs.")
        return linked_count
    except Exception as e:
        # The calling 'with' block will handle the rollback.
        logging.error(f"Error during SCI linking process: {e}")
        raise