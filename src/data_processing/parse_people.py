import duckdb
import xml.etree.ElementTree as ET
import sqlite3
import pandas as pd

def parse_mdb_xml(xml_path):
    # Lists to store records
    mdb_records = []
    wp_records = []
    
    # Parse XML file
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    # Iterate through each MDB entry
    for mdb in root.findall('MDB'):
        mdb_id = mdb.find('ID').text
        
        # Basic info for MDB table
        mdb_record = {
            'id': mdb_id,
            'nachname': mdb.find('.//NACHNAME').text,
            'vorname': mdb.find('.//VORNAME').text,
            'titel': ' '.join(filter(None, [
                mdb.find('.//ANREDE_TITEL').text,
                mdb.find('.//AKAD_TITEL').text
            ])),
            'geburtsdatum': mdb.find('.//GEBURTSDATUM').text,
            'sterbdatum': mdb.find('.//STERBEDATUM').text,
            'geschlecht': mdb.find('.//GESCHLECHT').text,
            'partei': mdb.find('.//PARTEI_KURZ').text,
            'beruf': mdb.find('.//BERUF').text,
        }
        mdb_records.append(mdb_record)
        
        # Election periods for WP table
        for wp in mdb.findall('.//WAHLPERIODE'):
            wp_record = {
                'mdb_id': mdb_id,
                'wp': wp.find('WP').text,
                'von': wp.find('MDBWP_VON').text,
                'bis': wp.find('MDBWP_BIS').text,
                'mandatsart': wp.find('MANDATSART').text,
                'wkr_nummer': wp.find('WKR_NUMMER').text,
                'wkr_land': wp.find('WKR_LAND').text
            }
            wp_records.append(wp_record)
    
    # Create DataFrames
    df_mdb = pd.DataFrame(mdb_records)
    df_wp = pd.DataFrame(wp_records)
    
    # Reorder columns
    mdb_columns = [
        'id', 'nachname', 'vorname', 'titel', 'geburtsdatum', 'sterbdatum',
        'geschlecht', 'partei', 'beruf'
    ]
    wp_columns = [
        'mdb_id', 'wp', 'von', 'bis', 'mandatsart', 'wkr_nummer', 'wkr_land'
    ]
    
    df_mdb = df_mdb[mdb_columns]
    df_wp = df_wp[wp_columns]
    df_mdb['full_name'] = df_mdb['nachname'] + ' ' + df_mdb['vorname']
    
    return df_mdb, df_wp

if __name__ == '__main__':

    conn = sqlite3.connect('/home/laurinbrechter/Documents/Code/project-bundestag/data/processed/bundestag.db')
    path = '/home/laurinbrechter/Documents/Code/project-bundestag/data/MdB-Stammdaten/MDB_STAMMDATEN.XML'
    df_mdb, df_wp = parse_mdb_xml(path)


    # SQLite connection and save
    conn_sqlite = sqlite3.connect('/home/laurinbrechter/Documents/Code/project-bundestag/data/processed/bundestag.db')
    df_mdb.to_sql('mdb_stammdaten', conn_sqlite, if_exists='replace', index=False)
    df_wp.to_sql('mdb_wahlperioden', conn_sqlite, if_exists='replace', index=False)
    conn_sqlite.close()

# DuckDB connection and save
    conn_duck = duckdb.connect('/home/laurinbrechter/Documents/Code/project-bundestag/data/processed/bundestag.duckdb')
    conn_duck.execute("CREATE OR REPLACE TABLE mdb_stammdaten AS SELECT * FROM df_mdb")
    conn_duck.execute("CREATE OR REPLACE TABLE mdb_wahlperioden AS SELECT * FROM df_wp")
    conn_duck.close()

    conn.close()