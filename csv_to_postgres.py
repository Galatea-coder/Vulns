from sqlalchemy import create_engine, text
import pandas as pd

# Database connection setup
SQLALCHEMY_DATABASE_URL = "postgresql://db_root:Mumbai2020#@localhost:5432/postgres"
engine = create_engine(SQLALCHEMY_DATABASE_URL)

def upload_to_postgres(df, table_name):
    try:
        # Upload data to PostgreSQL
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        # Verify upload by checking the number of rows
        with engine.connect() as connection:
            result = connection.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
            print(f"Successfully uploaded {row_count} records to table '{table_name}'.")

    except Exception as e:
        print(f"Failed to upload data to table '{table_name}': {e}")

# Load and publish data to PostgreSQL tables

# plextrac_findings
df = pd.read_excel('plextrac_findings.xlsx', sheet_name='in')
df['createdAt'] = pd.to_datetime(df['createdAt'], unit='ms')
df['last_update'] = pd.to_datetime(df['last_update'], unit='ms')
df['age'] = (df['last_update'] - df['createdAt']).dt.days
upload_to_postgres(df, 'plextrac_findings')

# plextrac_asset_finding_map
df = pd.read_excel('plextrac_asset_finding_map.xlsx', sheet_name='in')
upload_to_postgres(df, 'plextrac_asset_finding_map')

# plextrac_asset_list
df = pd.read_excel('plextrac_asset_list.xlsx', sheet_name='in')
upload_to_postgres(df, 'plextrac_asset_list')

# plextrac_clients_list
df = pd.read_excel('plextrac_clients_list.xlsx', sheet_name='in')
upload_to_postgres(df, 'plextrac_clients_list')

# plextrac_finding_cve_map
df = pd.read_excel('plextrac_finding_cve_map.xlsx', sheet_name='in')
upload_to_postgres(df, 'plextrac_finding_cve_map')



df = pd.read_excel('cve_mitre.xlsx', sheet_name='Sheet1')
upload_to_postgres(df, 'cve_mitre')

df = pd.read_csv('mandiant_vulns.csv')
upload_to_postgres(df, 'mandiant_vulns')

df = pd.read_csv('tenable_output(extracted).csv')
upload_to_postgres(df, 'tenable_output')

df = pd.read_excel('intel.xlsx', sheet_name='Sheet1')
upload_to_postgres(df, 'intel')
