import os
import traceback
import logging
import json
import io
import csv
import shutil
import subprocess
from urllib.parse import urlparse
from datetime import datetime
from flask import Blueprint, render_template, request, flash, redirect, url_for, send_from_directory, Response, current_app, jsonify
from werkzeug.utils import secure_filename
from db_client import DBClient
from init_db import create_schema as run_init_db

admin_bp = Blueprint('admin', __name__, template_folder='../../templates', url_prefix='/admin')

BACKUP_DIR = os.path.join(os.getcwd(), 'backups')
os.makedirs(BACKUP_DIR, exist_ok=True)

APP_BACKUP_DIR = os.path.join(os.getcwd(), 'app_backbacks')
os.makedirs(APP_BACKUP_DIR, exist_ok=True)

CONFIG_EXPORT_DIR = os.path.join(os.getcwd(), 'config_exports')
os.makedirs(CONFIG_EXPORT_DIR, exist_ok=True)

# --- Utility Functions (moved or recreated here for completeness) ---

def _get_db_connection_params():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable is not set.")
    parsed_url = urlparse(db_url)
    return {
        'db_name': parsed_url.path[1:],
        'db_user': parsed_url.username,
        'db_password': parsed_url.password,
        'db_host': parsed_url.hostname,
        'db_port': parsed_url.port or 5432
    }

def _run_subprocess_command(cmd, error_message):
    db_password = _get_db_connection_params()['db_password']
    env = os.environ.copy()
    env['PGPASSWORD'] = db_password
    try:
        logging.info(f"Executing command: {' '.join(cmd[:-1])} <password_hidden> {cmd[-1] if cmd[-1].startswith('-f') else ''}")
        result = subprocess.run(cmd, capture_output=True, text=True, check=True, env=env)
        logging.info(f"Command stdout: {result.stdout}")
        if result.stderr:
            logging.warning(f"Command stderr: {result.stderr}")
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"{error_message}: {e.stderr}")
        raise Exception(f"{error_message}: {e.stderr}")
    except Exception as e:
        logging.error(f"{error_message}: {e}")
        raise
    finally:
        pass

def _perform_full_db_backup(db_params):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"reconciliation_db_backup_{timestamp}.sql"
    backup_filepath = os.path.join(BACKUP_DIR, backup_filename)
    cmd = [
        'pg_dump',
        '-h', db_params['db_host'],
        '-p', str(db_params['db_port']),
        '-U', db_params['db_user'],
        '-F', 'p',
        '-d', db_params['db_name'],
        '-f', backup_filepath
    ]
    _run_subprocess_command(cmd, "pg_dump command failed")
    return backup_filepath

def _perform_data_only_backup(db_params):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"reconciliation_db_data_only_backup_{timestamp}.sql"
    backup_filepath = os.path.join(BACKUP_DIR, backup_filename)
    cmd = [
        'pg_dump',
        '--data-only',
        '-h', db_params['db_host'],
        '-p', str(db_params['db_port']),
        '-U', db_params['db_user'],
        '-F', 'p',
        '-d', db_params['db_name'],
        '-f', backup_filepath
    ]
    _run_subprocess_command(cmd, "pg_dump data-only command failed")
    return backup_filepath

def _perform_data_only_restore(db_params, backup_filepath):
    restore_cmd = [
        'psql',
        '-h', db_params['db_host'],
        '-p', str(db_params['db_port']),
        '-U', db_params['db_user'],
        '-d', db_params['db_name'],
        '-f', backup_filepath
    ]
    _run_subprocess_command(restore_cmd, "psql restore command failed")

def _perform_full_db_restore(db_params, backup_filepath):
    disconnect_cmd = [
        'psql',
        '-h', db_params['db_host'],
        '-p', str(db_params['db_port']),
        '-U', db_params['db_user'],
        '-d', 'postgres',
        '-c', f"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = '{db_params['db_name']}' AND pid <> pg_backend_pid();"
    ]
    drop_db_cmd = [
        'dropdb',
        '-h', db_params['db_host'],
        '-p', str(db_params['db_port']),
        '-U', db_params['db_user'],
        db_params['db_name']
    ]
    create_db_cmd = [
        'createdb',
        '-h', db_params['db_host'],
        '-p', str(db_params['db_port']),
        '-U', db_params['db_user'],
        db_params['db_name']
    ]
    restore_cmd = [
        'psql',
        '-h', db_params['db_host'],
        '-p', str(db_params['db_port']),
        '-U', db_params['db_user'],
        '-d', db_params['db_name'],
        '-f', backup_filepath
    ]
    logging.info("Terminating existing connections and dropping database...")
    _run_subprocess_command(disconnect_cmd, "Failed to terminate connections")
    _run_subprocess_command(drop_db_cmd, "Failed to drop database")
    logging.info("Database dropped. Creating new database...")
    _run_subprocess_command(create_db_cmd, "Failed to create database")
    logging.info("New database created. Starting restore...")
    _run_subprocess_command(restore_cmd, "psql restore command failed")


def export_mapping_data(export_dir, format):
    """
    Exports all mapping rules and lines from the database.
    :param export_dir: The directory to save the exported file.
    :param format: 'json' or 'csv'.
    :return: The filepath of the exported file.
    """
    if not os.path.exists(export_dir):
        os.makedirs(export_dir)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    
    with DBClient() as client:
        mappings_sql = "SELECT * FROM import_mappings ORDER BY mapping_name;"
        mappings = client._execute_query(mappings_sql, fetch='all')

        lines_sql = "SELECT * FROM import_mapping_lines ORDER BY mapping_name_id, id;"
        mapping_lines = client._execute_query(lines_sql, fetch='all')

    if not mappings:
        return None

    if format == 'json':
        export_data = []
        for mapping in mappings:
            mapping_dict = dict(mapping)
            mapping_dict['lines'] = [
                dict(line) for line in mapping_lines if line['mapping_name_id'] == mapping['id']
            ]
            export_data.append(mapping_dict)
            
        filename = f"all_mappings_export_{timestamp}.json"
        filepath = os.path.join(export_dir, filename)
        with open(filepath, 'w') as f:
            json.dump(export_data, f, indent=4)
        return filepath

    elif format == 'csv':
        filename = f"all_mappings_export_{timestamp}.csv"
        filepath = os.path.join(export_dir, filename)

        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            fieldnames = [
                'mapping_name', 'mapping_description', 'supplier_id',
                'line_id', 'source_csv_column', 'operation', 'value_a', 'value_b'
            ]
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()

            for mapping in mappings:
                lines = [line for line in mapping_lines if line['mapping_name_id'] == mapping['id']]
                if not lines:
                    writer.writerow({
                        'mapping_name': mapping['mapping_name'],
                        'mapping_description': mapping['description'],
                        'supplier_id': mapping['supplier_id'],
                    })
                for line in lines:
                    writer.writerow({
                        'mapping_name': mapping['mapping_name'],
                        'mapping_description': mapping['description'],
                        'supplier_id': mapping['supplier_id'],
                        'line_id': line['id'],
                        'source_csv_column': line['source_csv_column'],
                        'operation': line['operation'],
                        'value_a': line['value_a'],
                        'value_b': line['value_b']
                    })
        return filepath

    return None

def import_mapping_data(filepath, supplier_id):
    pass

def check_schema_status(client):
    pass

def export_db_schema(client):
    """
    Exports the database schema to a JSON file.
    :param client: An instance of DBClient.
    :return: The filepath of the exported schema file.
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    export_filename = f"exported_schema_{timestamp}.json"
    export_filepath = os.path.join(CONFIG_EXPORT_DIR, export_filename)

    schema_sql = """
    SELECT
        table_name,
        column_name,
        data_type,
        is_nullable,
        column_default
    FROM
        information_schema.columns
    WHERE
        table_schema = 'public'
    ORDER BY
        table_name,
        column_name;
    """

    constraints_sql = """
    SELECT
        tc.table_name,
        tc.constraint_type,
        kcu.column_name,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM
        information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
        JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
    WHERE
        tc.table_schema = 'public'
    ORDER BY
        tc.table_name,
        tc.constraint_type,
        kcu.column_name;
    """
    
    tables = client._execute_query(schema_sql, fetch='all')
    constraints = client._execute_query(constraints_sql, fetch='all')

    schema = {}
    for table in tables:
        table_name = table['table_name']
        if table_name not in schema:
            schema[table_name] = {'columns': [], 'constraints': []}
        schema[table_name]['columns'].append({
            'column_name': table['column_name'],
            'data_type': table['data_type'],
            'is_nullable': table['is_nullable'],
            'column_default': table['column_default']
        })

    for constraint in constraints:
        table_name = constraint['table_name']
        if table_name in schema:
            schema[table_name]['constraints'].append(dict(constraint))

    with open(export_filepath, 'w') as f:
        json.dump(schema, f, indent=4)
        
    return export_filepath

@admin_bp.route("/", methods=["GET"])
def admin_dashboard():
    db_backup_files = []
    try:
        files = os.listdir(BACKUP_DIR)
        db_backup_files = sorted(
            [f for f in files if f.endswith('.sql')],
            key=lambda f: os.path.getmtime(os.path.join(BACKUP_DIR, f)),
            reverse=True
        )
    except Exception as e:
        flash(f"Error listing DB backup files: {e}", "error")
        logging.error(f"Error listing DB backup files: {traceback.format_exc()}")
    app_backup_files = []
    try:
        files = os.listdir(APP_BACKUP_DIR)
        app_backup_files = sorted(
            [f for f in files if f.endswith('.zip')],
            key=lambda f: os.path.getmtime(os.path.join(APP_BACKUP_DIR, f)),
            reverse=True
        )
    except Exception as e:
        flash(f"Error listing app backup files: {e}", "error")
        logging.error(f"Error listing app backup files: {traceback.format_exc()}")
    config_export_files = []
    try:
        files = os.listdir(CONFIG_EXPORT_DIR)
        config_export_files = sorted(
            [f for f in files if f.endswith(('.json', '.csv'))],
            key=lambda f: os.path.getmtime(os.path.join(CONFIG_EXPORT_DIR, f)),
            reverse=True
        )
    except Exception as e:
        flash(f"Error listing configuration export files: {e}", "error")
        logging.error(f"Error listing config export files: {traceback.format_exc()}")
    return render_template("admin.html",
                           db_backup_files=db_backup_files,
                           app_backup_files=app_backup_files,
                           config_export_files=config_export_files
                           )

@admin_bp.route("/init_db", methods=["POST"])
def init_db_route():
    try:
        run_init_db()
        flash("Database schema wiped and re-initialized successfully!", "success")
    except Exception as e:
        flash(f"An error occurred while initializing the database schema: {e}", "error")
        logging.error(f"[ADMIN INIT DB PROCESS] Error: {traceback.format_exc()}")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/run_sql_script", methods=["POST"])
def run_sql_script():
    if 'sql_file' not in request.files:
        flash("No SQL file provided.", "error")
        return redirect(url_for('admin.admin_dashboard'))
    sql_file = request.files['sql_file']
    if sql_file.filename == '':
        flash("No file selected.", "error")
        return redirect(url_for('admin.admin_dashboard'))
    if not sql_file.filename.lower().endswith('.sql'):
        flash("Invalid file type. Please upload a .sql file.", "error")
        return redirect(url_for('admin.admin_dashboard'))
    temp_filepath = None
    try:
        temp_filename = secure_filename(sql_file.filename)
        temp_filepath = os.path.join(current_app.config.get('UPLOAD_FOLDER', '/tmp'), temp_filename)
        sql_file.save(temp_filepath)
        with open(temp_filepath, 'r', encoding='utf-8') as f:
            sql_content = f.read()
        with DBClient() as client:
            client._execute_raw_sql(sql_content)
        flash(f"SQL script '{sql_file.filename}' executed successfully.", "success")
    except Exception as e:
        flash(f"An error occurred while executing SQL script '{sql_file.filename}': {e}", "error")
        logging.error(f"Error executing SQL script: {traceback.format_exc()}")
    finally:
        if temp_filepath and os.path.exists(temp_filepath):
            os.remove(temp_filepath)
            logging.info(f"Cleaned up temporary SQL file: {temp_filepath}")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/update_db", methods=["POST"])
def update_db_route():
    db_params = _get_db_connection_params()
    temp_data_backup_filepath = None
    try:
        flash("Starting data-only database backup...", "info")
        temp_data_backup_filepath = _perform_data_only_backup(db_params)
        flash("Data-only database backup completed successfully.", "success")
        flash("Initializing database schema...", "info")
        run_init_db()
        flash("Database schema re-initialized successfully!", "success")
        flash("Restoring all data from backup...", "info")
        _perform_data_only_restore(db_params, temp_data_backup_filepath)
        flash("All data restored successfully!", "success")
    except Exception as e:
        flash(f"An error occurred during the DB update process: {e}", "error")
        logging.error(f"[ADMIN DB UPDATE PROCESS] Error: {traceback.format_exc()}")
    finally:
        if temp_data_backup_filepath and os.path.exists(temp_data_backup_filepath):
            os.remove(temp_data_backup_filepath)
            logging.info(f"Cleaned up temporary data-only DB backup file: {temp_data_backup_filepath}")
    return redirect(url_for("admin.admin_dashboard"))

@admin_bp.route("/data/export_schema", methods=["POST"])
def export_db_schema_route():
    try:
        with DBClient() as client:
            filepath = export_db_schema(client)
            flash(f"Database schema exported successfully to '{os.path.basename(filepath)}'.", "success")
            return redirect(url_for('admin.admin_dashboard'))
    except Exception as e:
        flash(f"Error exporting schema: {e}", "error")
        logging.error(f"Error during schema export: {traceback.format_exc()}")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/data/export_mappings_csv", methods=["GET"])
def export_mappings_csv():
    try:
        filepath = export_mapping_data(format='csv', export_dir=CONFIG_EXPORT_DIR)
        flash(f"Successfully exported mappings to {os.path.basename(filepath)}", "success")
    except Exception as e:
        flash(f"Error exporting mappings to CSV: {e}", "error")
        logging.error(f"Error during CSV mapping export: {traceback.format_exc()}")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/data/export_mappings_json", methods=["GET"])
def export_mappings_json():
    try:
        filepath = export_mapping_data(format='json', export_dir=CONFIG_EXPORT_DIR)
        flash(f"Successfully exported mappings to {os.path.basename(filepath)}", "success")
    except Exception as e:
        flash(f"Error exporting mappings to JSON: {e}", "error")
        logging.error(f"Error during JSON mapping export: {traceback.format_exc()}")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/backup_full_db", methods=["POST"])
def backup_full_db():
    db_params = _get_db_connection_params()
    try:
        backup_filepath = _perform_full_db_backup(db_params)
        backup_filename = os.path.basename(backup_filepath)
        flash(f"Full database backup created successfully: {backup_filename}", "success")
    except Exception as e:
        flash(f"An error occurred during full database backup: {e}", "error")
        logging.error(f"Full DB backup error: {traceback.format_exc()}")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/restore_full_db", methods=["POST"])
def restore_full_db():
    if 'backup_file' not in request.files:
        flash("No backup file provided.", "error")
        return redirect(url_for('admin.admin_dashboard'))
    file = request.files['backup_file']
    if file.filename == '':
        flash("No file selected.", "error")
        return redirect(url_for('admin.admin_dashboard'))
    temp_filepath = None
    db_params = _get_db_connection_params()
    try:
        temp_filename = secure_filename(file.filename)
        temp_filepath = os.path.join(BACKUP_DIR, temp_filename)
        file.save(temp_filepath)
        flash(f"Starting database restore from {file.filename}...", "info")
        _perform_full_db_restore(db_params, temp_filepath)
    except Exception as e:
        flash(f"An error occurred during full database restore: {e}", "error")
        logging.error(f"Full DB restore error: {traceback.format_exc()}")
    finally:
        if temp_filepath and os.path.exists(temp_filepath):
            os.remove(temp_filepath)
            logging.info(f"Cleaned up temporary restore file: {temp_filepath}")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/download_backup/<path:filename>", methods=["GET"])
def download_backup(filename):
    try:
        safe_filename = secure_filename(filename)
        return send_from_directory(BACKUP_DIR, safe_filename, as_attachment=True)
    except Exception as e:
        flash(f"Error downloading backup file: {e}", "error")
        logging.error(f"Error downloading backup: {traceback.format_exc()}")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/download_config_export/<path:filename>", methods=["GET"])
def download_config_export(filename):
    try:
        safe_filename = secure_filename(filename)
        return send_from_directory(CONFIG_EXPORT_DIR, safe_filename, as_attachment=True)
    except Exception as e:
        flash(f"Error downloading configuration file: {e}", "error")
        logging.error(f"Error downloading config export: {traceback.format_exc()}")
        return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/delete_selected_db_backups", methods=["POST"])
def delete_selected_db_backups():
    selected_files = request.form.getlist('filenames[]')
    deleted_count = 0
    errors = []
    for filename in selected_files:
        file_path = os.path.join(BACKUP_DIR, secure_filename(filename))
        try:
            if os.path.exists(file_path) and os.path.commonprefix([file_path, BACKUP_DIR]) == BACKUP_DIR:
                os.remove(file_path)
                deleted_count += 1
            else:
                errors.append(f"File '{filename}' not found or invalid path.")
        except Exception as e:
            errors.append(f"Error deleting '{filename}': {e}")
            logging.error(f"Error deleting selected DB backup file {filename}: {traceback.format_exc()}")
    if deleted_count > 0:
        flash(f"Successfully deleted {deleted_count} database backup file(s).", "success")
    if errors:
        for error in errors:
            flash(error, "error")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/delete_selected_config_exports", methods=["POST"])
def delete_selected_config_exports():
    selected_files = request.form.getlist('filenames[]')
    deleted_count = 0
    errors = []
    for filename in selected_files:
        file_path = os.path.join(CONFIG_EXPORT_DIR, secure_filename(filename))
        try:
            if os.path.exists(file_path) and os.path.commonprefix([file_path, CONFIG_EXPORT_DIR]) == CONFIG_EXPORT_DIR:
                os.remove(file_path)
                deleted_count += 1
            else:
                errors.append(f"File '{filename}' not found or invalid path.")
        except Exception as e:
            errors.append(f"Error deleting '{filename}': {e}")
            logging.error(f"Error deleting selected config export file {filename}: {traceback.format_exc()}")
    if deleted_count > 0:
        flash(f"Successfully deleted {deleted_count} configuration export file(s).", "success")
    if errors:
        for error in errors:
            flash(error, "error")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/backup_full_app", methods=["POST"])
def backup_full_app():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename_base = f"reconciliation_app_backup_{timestamp}"
    backup_filepath_base = os.path.join(APP_BACKUP_DIR, backup_filename_base)
    source_dir = os.getcwd()
    try:
        logging.info(f"Starting full application backup of '{source_dir}'...")
        shutil.make_archive(backup_filepath_base, 'zip', source_dir)
        flash(f"Full application backup '{backup_filename_base}.zip' created successfully.", "success")
    except Exception as e:
        flash(f"An error occurred during full application backup: {e}", "error")
        logging.error(f"Full app backup error: {traceback.format_exc()}")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/restore_full_app", methods=["POST"])
def restore_full_app():
    flash("Full application restore is a high-risk operation and is not enabled in this interface. Please perform this action manually on the server.", "warning")
    return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/download_app_backup/<path:filename>", methods=["GET"])
def download_app_backup(filename):
    try:
        safe_filename = secure_filename(filename)
        return send_from_directory(APP_BACKUP_DIR, safe_filename, as_attachment=True)
    except Exception as e:
        flash(f"Error downloading application backup file: {e}", "error")
        logging.error(f"Error downloading app backup: {traceback.format_exc()}")
        return redirect(url_for('admin.admin_dashboard'))

@admin_bp.route("/delete_selected_app_backups", methods=["POST"])
def delete_selected_app_backups():
    selected_files = request.form.getlist('filenames[]')
    deleted_count = 0
    errors = []
    for filename in selected_files:
        file_path = os.path.join(APP_BACKUP_DIR, secure_filename(filename))
        try:
            if os.path.exists(file_path) and os.path.commonprefix([file_path, APP_BACKUP_DIR]) == APP_BACKUP_DIR:
                os.remove(file_path)
                deleted_count += 1
            else:
                errors.append(f"File '{filename}' not found or invalid path.")
        except Exception as e:
            errors.append(f"Error deleting '{filename}': {e}")
            logging.error(f"Error deleting selected app backup file {filename}: {traceback.format_exc()}")
    if deleted_count > 0:
        flash(f"Successfully deleted {deleted_count} application backup file(s).", "success")
    if errors:
        for error in errors:
            flash(error, "error")
    return redirect(url_for('admin.admin_dashboard'))