import os
import zipfile
import gdown
import yaml
import logging


def setup_logging(log_file="data_ingestion.log"):
    """Sets up logging with DEBUG level."""
    os.makedirs("logs", exist_ok=True)
    log_path = os.path.join("logs", log_file)

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s | %(levelname)s | %(module)s | %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler()
        ]
    )
    logging.debug("Logging initialized.")


def get_config():
    """Reads and returns the configuration data from the YAML file."""
    config_path = r'src\cnnClassifier\config\config.yaml'

    logging.debug(f"Loading config from {config_path}")

    if not os.path.exists(config_path):
        logging.error(f"Config file not found at {config_path}")
        return None

    try:
        with open(config_path, 'r') as f:
            data = yaml.safe_load(f)
            logging.info("Configuration loaded successfully.")
            return data
    except Exception as e:
        logging.error(f"Error loading config: {e}")
        return None


def download_file(url, local_data_file):
    try:
        logging.info("Starting file download...")
        os.makedirs("artifacts/data_ingestion", exist_ok=True)

        file_id = url.split("/")[-2]
        prefix = "https://drive.google.com/uc?export=download&id="

        logging.debug(f"Downloading from URL: {url}")
        logging.debug(f"File ID extracted: {file_id}")

        gdown.download(prefix + file_id, local_data_file, quiet=False)

        logging.info(f"File downloaded to {local_data_file}")
        return local_data_file

    except Exception as e:
        logging.error(f"Download failed: {e}")
        raise


def extract_zip_file(zip_path, unzip_dir):
    """Extracts ZIP file to target directory."""
    try:
        logging.info(f"Extracting ZIP file: {zip_path}")

        os.makedirs(unzip_dir, exist_ok=True)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(unzip_dir)

        logging.info(f"Extraction completed. Files saved to: {unzip_dir}")

    except Exception as e:
        logging.error(f"Zip extraction failed: {e}")
        raise


def main():
    setup_logging()

    config = get_config()
    if config is None:
        logging.critical("Config loading failed. Stopping program.")
        return

    url = config["data_ingestion"]["source_URL"]
    local_data_file = config["data_ingestion"]["local_data_file"]
    unzip_dir = config["data_ingestion"]["unzip_dir"]

    logging.debug("Starting data ingestion pipeline...")

    zip_path = download_file(url, local_data_file)
    extract_zip_file(zip_path, unzip_dir)

    logging.info("Data ingestion pipeline completed successfully.")


if __name__ == "__main__":
    main()
