import logging
import os
import subprocess
import time

# Configure logging
logging.basicConfig(filename='/app/logs/elt_script.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s'
)

def wait_for_postgres(host, max_retries=5, base_delay_seconds=1):
    """Wait for PostgreSQL to become available."""
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True
            )
            if "accepting connections" in result.stdout:
                logging.info("Successfully connected to {host}!")
                return True
        except subprocess.CalledProcessError as e:
            retries += 1
            wait_time = base_delay_seconds * (2 ** retries)
            logging.warning(f"Error connecting to {host}: {e}. Retrying in {wait_time} seconds...")
            
            logging.info(
                f"Retrying in {wait_time} seconds... (Attempt {retries}/{max_retries})"
            )
            time.sleep(wait_time)
    logging.error("Max retries reached. \nFailed to connect to {host} within the specified timeout. \nExiting...")
    return False

# Get environment vvariables for the source and destination databases
# Configuration for the source PostgreSQL database
source_config = {
    'host': os.getenv('SOURCE_DB_HOST'),
    'user': os.getenv('SOURCE_DB_USER'),
    'password': os.getenv('SOURCE_DB_PASSWORD'),
    'dbname': os.getenv('SOURCE_DB_NAME')
}

# Configuration for the destination PostgreSQL database
destination_config = {
    #'host': os.getenv('DEST_DB_HOST'),
    'host': 'destination_postgres',
    'user': os.getenv('DEST_DB_USER', 'postgres'),
    'password': os.getenv('DEST_DB_PASSWORD', 'secret'),
    'dbname': os.getenv('DEST_DB_NAME', 'destination_db')
}

# Use the function before running the ELT process
if not wait_for_postgres(host=source_config['host']):
    logging.error("Failed to connect to source database. Exiting...")
    exit(1)
    
if not wait_for_postgres(host=destination_config['host']):
    logging.error("Failed to connect to destination database. Exiting...")
    exit(1)

logging.info("Starting ELT process...")

# Use pg_dump to dump the source database to a SQL file
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'  # Do not prompt for password
]

try:
    # Set the PGPASSWORD environment variable to avoid password prompt
    subprocess_env = dict(PGPASSWORD=source_config['password'])
    # Execute the dump command
    subprocess.run(dump_command, env=subprocess_env, check=True)
    logging.info("Data successfully dumped from source database.")
except:
    logging.error(f"Failed to dump data from source database.")
    exit(1)

# Use psql to load the dumped SQL file into the destination database
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

try:
    # Set the PGPASSWORD environment variable for the destination database
    subprocess_env = dict(PGPASSWORD=destination_config['password'])
    # Execute the load command
    subprocess.run(load_command, env=subprocess_env, check=True)
    logging.info("Data successfully loaded into destination database.")
except subprocess.CalledProcessError as e:
    logging.error(f"Failed to load data into destination database. Command: {e.command}, Return code: {e.returncode}")
    logging.error(f"Command output: {e.output}")
    exit(1)

# Remove the dump file after successful load
try:
    os.remove('data_dump.sql')
    logging.info("Intermediate dump file removed successfully.")
except OSError as e:
    logging.error(f"Failed to remove dump file. {e}")
    exit(1)

logging.info("Ending ELT script...")