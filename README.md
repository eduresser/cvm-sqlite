# CVM-SQLite

CVM-SQLite is a Python tool for downloading, processing, and storing data from the Brazilian Securities and Exchange Commission (CVM - Comissão de Valores Mobiliários) in a SQLite database.

## Features

- Automatically downloads data from the CVM repository
- Processes and structures data according to provided schemas
- Stores data in a SQLite database
- Supports incremental updates

## What can be obtained with CVM-SQLite?

CVM-SQLite facilitates access to a comprehensive range of official documents from various financial entities regulated by the Brazilian Securities and Exchange Commission (CVM). These include:

### `CIA_ABERTA` (Companhias Abertas) - Public Companies

- DFP (Demonstrações Financeiras Padronizadas) - Standardized Financial Statements
- ITR (Informações Trimestrais) - Quarterly Information Reports
- FRE (Formulário de Referência) - Reference Form
- FCA (Formulário Cadastral) - Registration Form
- Additional relevant documentation

### `FI` (Fundos de Investimento) - Investment Funds

- Financial Statements
- Periodic Reports
- Balance Sheets
- CDA (Composição e Diversificação das Aplicações) - Portfolio Composition and Diversification
- Other pertinent fund documentation

### `FII` (Fundos de Investimento Imobiliário) - REIT (Real Estate Investment Trusts)

- Periodic Reports
- DFIN (Demonstrações Financeiras de Fundos de Investimentos) - Investment Fund Financial Statements

### `SECURIT` (Securitizadoras) - Securitization Companies

- CRA (Certificado de Recebíveis do Agronegócio) - Agribusiness Receivables Certificate
- CRI (Certificado de Recebíveis Imobiliários) - Real Estate Receivables Certificate
- OTS (Operações de Transferência de Saldos) - Balance Transfer Operations

### Additional Financial Instruments and Entities

- Other investment funds
- Equity Crowdfunding Platforms
- Auditing Reports
- Registration relating to Foreign Companies operating in Brazil
- Other relevant financial and regulatory documents

## Installation

You can install CVM-SQLite using pip:

```bash
pip install cvm-sqlite
```

## Usage

```python
from cvm_sqlite import CVMDataProcessor

# Initialize the processor
processor = CVMDataProcessor(
    db_path='path/to/your/database.db',
    cvm_url='https://dados.cvm.gov.br/dados/CIA_ABERTA',
    verbose=True
)

# Run the processor
processor.run()

# Now you can use the processor object to run queries
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
```

## Parameters

- `db_path`: Path to the SQLite database file.
- `cvm_url`: URL of the CVM directory (optional, default: https://dados.cvm.gov.br/dados/CIA_ABERTA).
- `verbose`: Enables detailed output (optional, default: `False`).

**Note:** It is mandatory that the CVM directory includes a **META** folder at any level, which contains the table schemas.

## How it works

If the database doesn't exist, it will be created.
From the URL provided, all files and directories that can be accessed from this URL will be mapped.
The tables are created according to the schema file provided by the CVM itself.
If the database already exists, only necessary updates will be made, processing only new files or those whose last modification date has changed.

## License

This project is licensed under the MIT License.
