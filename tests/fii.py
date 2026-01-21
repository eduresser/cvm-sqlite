from cvm_sqlite import CVMDataProcessor

# Initialize the processor
processor = CVMDataProcessor(
    db_path='.cache/fii.db',
    cvm_url='https://dados.cvm.gov.br/dados/FII/',
    verbose=False
)

print('Running processor...')

processor.run()

print('Processor finished.')