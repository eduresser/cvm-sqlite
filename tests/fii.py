from cvm_sqlite import CVMDataProcessor

# Initialize the processor
processor = CVMDataProcessor(
    db_path='.cache/fii.db',
    cvm_url='https://dados.cvm.gov.br/dados/FII/',
    verbose=True
)

processor.run()