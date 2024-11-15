from cvm_sqlite import CVMDataProcessor

processor = CVMDataProcessor(
    db_path='database.db',
    cvm_url='https://dados.cvm.gov.br/dados/WRONG_PATH',
    verbose=False
)

processor.process()