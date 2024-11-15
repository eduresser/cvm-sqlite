from cvm_sqlite import CVMDataProcessor

processor = CVMDataProcessor(
    db_path='database.db',
    cvm_url='https://www.google.com',
    verbose=False
)

processor.process()