import os
import psycopg2
from urllib.parse import urlparse

def create_schema():
    """
    Connects to the PostgreSQL database and completely resets the schema.
    This script is DESTRUCTIVE and idempotent. It drops existing tables
    before creating them anew.

    *** FOR DEVELOPMENT USE ONLY ***
    """
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise ValueError("DATABASE_URL environment variable is not set.")

    conn = None
    try:
        print("Connecting to the PostgreSQL database...")
        conn = psycopg2.connect(database_url)
        cur = conn.cursor()

        print("--- WARNING: DESTRUCTIVE OPERATION ---")
        print("Dropping existing tables...")
        
        drop_sql = """
            DROP TABLE IF EXISTS scheduled_backups CASCADE;
            DROP TABLE IF EXISTS import_mapping_lines CASCADE;
            DROP TABLE IF EXISTS import_mappings CASCADE;
            DROP TABLE IF EXISTS supplier_invoice_lines CASCADE;
            DROP TABLE IF EXISTS supplier_invoice_headers CASCADE;
            DROP TABLE IF EXISTS circuit_invoice_links CASCADE;
            DROP TABLE IF EXISTS supplier_invoice_items CASCADE;
            DROP TABLE IF EXISTS cmdb_circuits CASCADE;
            DROP TABLE IF EXISTS service_catalog_items CASCADE;
            DROP TABLE IF EXISTS supplier_account CASCADE;
            DROP TABLE IF EXISTS customers CASCADE;
            DROP TABLE IF EXISTS suppliers CASCADE;
            DROP TABLE IF EXISTS loggingcontrol CASCADE;
            DROP TABLE IF EXISTS servicerates CASCADE;
            DROP TABLE IF EXISTS mapping_rules_metadata CASCADE;
        """
        cur.execute(drop_sql)

        print("Creating new database schema...")

        create_tables_sql = [
            """
            CREATE TABLE suppliers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL,
                type VARCHAR(255),
                account_manager_email VARCHAR(255),
                contact_person VARCHAR(255),
                supplier_short_name VARCHAR(255),
                other_names TEXT,
                override_name VARCHAR(255)
            );
            """,
            """
            CREATE TABLE customers (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            );
            """,
            """
            CREATE TABLE import_mappings (
                id SERIAL PRIMARY KEY,
                mapping_name VARCHAR(255) UNIQUE NOT NULL,
                notes TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                supplier_id INTEGER REFERENCES suppliers(id) ON DELETE CASCADE,
                description TEXT,
                display_order INTEGER,
                sample_csv_headers TEXT,
                sample_csv_first_row TEXT
            );
            """,
            """
            CREATE TABLE import_mapping_lines (
                id SERIAL PRIMARY KEY,
                mapping_name_id INTEGER REFERENCES import_mappings(id) ON DELETE CASCADE,
                name VARCHAR(255) NOT NULL,
                field_role VARCHAR(255),
                baserow_field_name VARCHAR(255),
                source_csv_column VARCHAR(255),
                transformation VARCHAR(255),
                transformation_args TEXT,
                ignore_match VARCHAR(255),
                source_type VARCHAR(255),
                source_link_table VARCHAR(255),
                source_link_field VARCHAR(255),
                formula_template TEXT,
                static_value TEXT,
                start_date TEXT,
                end_date TEXT
            );
            """,
            """
            CREATE TABLE supplier_account (
                id SERIAL PRIMARY KEY,
                account_number VARCHAR(255) NOT NULL,
                supplier_id INTEGER REFERENCES suppliers(id) ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE service_catalog_items (
                id SERIAL PRIMARY KEY,
                zeus_id VARCHAR(255),
                service_line VARCHAR(255),
                master_service_catalogue_item VARCHAR(255),
                netsuite_ru_item_code VARCHAR(255),
                unit VARCHAR(255),
                service_billing_method VARCHAR(255),
                billing_name VARCHAR(255),
                host_types VARCHAR(255),
                service_description TEXT,
                billable BOOLEAN,
                include_in_billing_run BOOLEAN,
                contract_number VARCHAR(255),
                service_key VARCHAR(255),
                datacom_cost_price NUMERIC,
                customer_standard_price NUMERIC,
                adjustment_percentage NUMERIC,
                customer_sell_price NUMERIC,
                additional_info TEXT,
                billable_quantity NUMERIC,
                pipeline_quantity NUMERIC,
                total_bill_price NUMERIC,
                customer_id INTEGER
            );
            """,
            """
            CREATE TABLE supplier_invoice_items (
                id SERIAL PRIMARY KEY,
                supplier_id INTEGER REFERENCES suppliers(id) ON DELETE CASCADE,
                billing_reference VARCHAR(255) UNIQUE NOT NULL,
                account_number_id INTEGER REFERENCES supplier_account(id) ON DELETE CASCADE,
                audit_date DATE,
                contract_start_date DATE,
                contract_end_date DATE,
                contract_term_in_months INTEGER,
                review_flag BOOLEAN DEFAULT FALSE,
                notes TEXT
            );
            """,
            """
            CREATE TABLE cmdb_circuits (
                id SERIAL PRIMARY KEY,
                zeus_id VARCHAR(255),
                circuit_id VARCHAR(255),
                circuit_name VARCHAR(255),
                status VARCHAR(255),
                site_name VARCHAR(255),
                site_code VARCHAR(255),
                telco VARCHAR(255),
                priority VARCHAR(255),
                circuit_type VARCHAR(255),
                circuit_service_type VARCHAR(255),
                circuit_termination_type VARCHAR(255),
                access VARCHAR(255),
                cir VARCHAR(255),
                pir VARCHAR(255),
                vlan VARCHAR(255),
                pe_host VARCHAR(255),
                ip_pe VARCHAR(255),
                ce_host VARCHAR(255),
                host VARCHAR(255),
                ip_router VARCHAR(255),
                interface VARCHAR(255),
                linknet VARCHAR(255),
                qosprofile VARCHAR(255),
                notes TEXT,
                netops_sensor_id VARCHAR(255),
                netflow BOOLEAN,
                contract_start DATE,
                contract_end DATE,
                exchange VARCHAR(255),
                region VARCHAR(255),
                activated DATE,
                decommissioned DATE,
                vendor_billing_end_date DATE,
                service_end_date DATE,
                datacom_last_billing_run_date DATE,
                datacom_billing_code VARCHAR(255),
                vendor_account_number VARCHAR(255),
                service_catalogue_item_imported VARCHAR(255),
                service_catalogue_item_linked_id INTEGER REFERENCES service_catalog_items(id) ON DELETE SET NULL,
                customer_id INTEGER REFERENCES customers(id) ON DELETE CASCADE,
                audit_date DATE,
                last_updated DATE,
                review_flag BOOLEAN DEFAULT FALSE,
                review_notes TEXT,
                review_notes_date TIMESTAMP WITH TIME ZONE,
                service_type VARCHAR(255),
                cir_pri VARCHAR(255)
            );
            """,
            """
            CREATE TABLE circuit_invoice_links (
                circuit_id INTEGER REFERENCES cmdb_circuits(id) ON DELETE CASCADE,
                invoice_item_id INTEGER REFERENCES supplier_invoice_items(id) ON DELETE CASCADE,
                PRIMARY KEY (circuit_id, invoice_item_id)
            );
            """,
            """
            CREATE TABLE supplier_invoice_headers (
                id SERIAL PRIMARY KEY,
                supplier_id INTEGER REFERENCES suppliers(id) ON DELETE CASCADE,
                invoice_number VARCHAR(255) UNIQUE NOT NULL,
                invoice_date DATE,
                due_date DATE,
                billing_month DATE, 
                total_amount DECIMAL(10, 2),
                tax_amount DECIMAL(10, 2),
                account_number_id INTEGER REFERENCES supplier_account(id) ON DELETE CASCADE
            );
            """,
            """
            CREATE TABLE supplier_invoice_lines (
                id SERIAL PRIMARY KEY,
                invoice_header_id INTEGER REFERENCES supplier_invoice_headers(id) ON DELETE CASCADE,
                item_id INTEGER REFERENCES supplier_invoice_items(id) ON DELETE CASCADE,
                unique_reference VARCHAR(255) UNIQUE NOT NULL,
                description TEXT,
                quantity INTEGER,
                unit_price DECIMAL(10, 2),
                total_amount DECIMAL(10, 2),
                start_date DATE,
                end_date DATE
            );
            """,
            """
            CREATE TABLE loggingcontrol (
                id SERIAL PRIMARY KEY,
                application VARCHAR(255) NOT NULL,
                state VARCHAR(50),
                level VARCHAR(50),
                description TEXT
            );
            """,
            """
            CREATE TABLE servicerates (
                id SERIAL PRIMARY KEY,
                rate_code VARCHAR(255) NOT NULL,
                notes TEXT,
                active BOOLEAN,
                supplier_id INTEGER REFERENCES suppliers(id) ON DELETE CASCADE,
                service_description TEXT,
                billing_type VARCHAR(255),
                unit_of_measure VARCHAR(255),
                rate_per_unit NUMERIC(16,6),
                effective_start_date DATE,
                effective_end_date DATE,
                currency VARCHAR(10)
            );
            """,
            """
            CREATE TABLE scheduled_backups (
                id SERIAL PRIMARY KEY,
                backup_name VARCHAR(255) UNIQUE NOT NULL,
                backup_type VARCHAR(50) NOT NULL,
                frequency VARCHAR(50) NOT NULL,
                time_of_day TIME,
                day_of_week INTEGER,
                day_of_month INTEGER,
                is_active BOOLEAN DEFAULT TRUE,
                last_run_status VARCHAR(50),
                last_run_timestamp TIMESTAMP WITH TIME ZONE,
                next_run_timestamp TIMESTAMP WITH TIME ZONE,
                notes TEXT
            );
            """,
            """
            CREATE TABLE mapping_rules_metadata (
                id SERIAL PRIMARY KEY,
                rule_name VARCHAR(255) UNIQUE NOT NULL,
                field_role VARCHAR(50) NOT NULL,
                destination_field VARCHAR(255) NOT NULL,
                source_type_options TEXT NOT NULL,
                rule_type VARCHAR(50) NOT NULL,
                is_required BOOLEAN DEFAULT FALSE NOT NULL,
                is_hidden BOOLEAN DEFAULT FALSE NOT NULL,
                is_base_rule BOOLEAN DEFAULT FALSE NOT NULL,
                link_table_lookup VARCHAR(255),
                link_field_lookup VARCHAR(255),
                formula_template TEXT,
                static_value TEXT,
                default_transformation VARCHAR(50),
                transformation_args TEXT
            );
            """
        ]

        for table_sql in create_tables_sql:
            cur.execute(table_sql)

        print("Creating indexes for performance...")
        
        create_indexes_sql = """
            CREATE INDEX idx_mappings_supplier_id ON import_mappings(supplier_id);
            CREATE INDEX idx_lines_mapping_id ON import_mapping_lines(mapping_name_id);
            CREATE INDEX idx_invoice_headers_supplier_id ON supplier_invoice_headers(supplier_id);
            CREATE INDEX idx_invoice_items_supplier_id ON supplier_invoice_items(supplier_id);
            CREATE INDEX idx_invoice_items_account_number_id ON supplier_invoice_items(account_number_id);
            CREATE INDEX idx_circuit_invoice_links_circuit_id ON circuit_invoice_links(circuit_id);
            CREATE INDEX idx_circuit_invoice_links_invoice_item_id ON circuit_invoice_links(invoice_item_id);
            CREATE UNIQUE INDEX idx_mapping_rules_metadata_rule_name ON mapping_rules_metadata(rule_name);
        """
        cur.execute(create_indexes_sql)
        
        conn.commit()
        print("Database schema created successfully.")

    except psycopg2.Error as error:
        print(f"Database Error: {error}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    confirm = input("This will DESTROY and recreate the database schema. Are you sure? (y/n): ")
    if confirm.lower() == 'y':
        create_schema()
    else:
        print("Operation cancelled.")