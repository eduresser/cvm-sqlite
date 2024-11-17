import json
from cvm_sqlite import CVMDataProcessor

processor = CVMDataProcessor(
    db_path='cvm_data/database.db',
    cvm_url='https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/CGVN/',
    verbose=False
)

processor.run()

results = processor.query("""
    SELECT
        CAST(STRFTIME('%Y', DT_REFER) AS INTEGER) AS exercise,
        DENOM_CIA AS company,
        VL_CONTA AS net_income
    FROM dfp_cia_aberta_DRE
    WHERE CNPJ_CIA = '00.000.000/0001-91'
        AND GRUPO_DFP = 'DF Consolidado - Demonstração do Resultado'
        AND ORDEM_EXERC = 'ÚLTIMO'
        AND (
            (CD_CONTA = '3.09' AND STRFTIME('%Y', DT_REFER) < '2020')
            OR (CD_CONTA = '3.11' AND STRFTIME('%Y', DT_REFER) >= '2020')
        )
    ORDER BY exercise
""")

print(json.dumps(results, indent=4))