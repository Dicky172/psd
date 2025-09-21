import pandas as pd
from sqlalchemy import create_engine

# --- GANTI INFORMASI DI BAWAH INI ---
db_user = 'postgres'  # ganti dengan username Anda
db_password = '170205' # ganti dengan password Anda
db_host = 'localhost' # ganti jika host berbeda (misal: alamat IP server)
db_port = '5433' # port default untuk PostgreSQL
db_name = 'Data_saya' # ganti dengan nama database Anda
table_name = 'Data_saya' # ganti dengan nama tabel yang berisi data IMDB
# ------------------------------------

# Membuat string koneksi (connection string)
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# Membuat engine koneksi ke database
engine = None
try:
    engine = create_engine(db_url)
    
    # Query SQL untuk mengambil semua data dari tabel
    sql_query = f'SELECT * FROM {table_name};'
    
    # Membaca data dari PostgreSQL ke dalam DataFrame pandas
    df = pd.read_sql(sql_query, engine)
    
    print("Berhasil terhubung dan mengambil data dari PostgreSQL.")
    print("\n5 Baris Data Teratas:")
    display(df.head())

except Exception as e:
    print(f"Gagal terhubung ke database: {e}")

finally:
    if engine is not None:
        engine.dispose() # Menutup koneksi