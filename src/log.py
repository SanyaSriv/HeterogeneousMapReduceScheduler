import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[
        logging.FileHandler("log.txt"),
        logging.StreamHandler() 
    ]
)

InfoLogger = logging.getLogger()
