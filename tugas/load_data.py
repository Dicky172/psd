import pandas as pd
from sqlalchemy import create_engine
import os

# --- PENGATURAN ---
db_user = 'postgres'
db_password = '123456789' 
db_host = 'localhost'
db_port = '5432'
db_name = 'sains_data'

csv_file_path = 'risk_factors_cervical_cancer.csv' 
table_name = 'penjualan'

# --- PROSES ---
try:
    # 1. Baca data dari file CSV menggunakan pandas
    print(f"Membaca file CSV: {csv_file_path}...")
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"Error: File '{csv_file_path}' tidak ditemukan.")
    
    df = pd.read_csv(csv_file_path)
    print("File CSV berhasil dibaca.")
    print("Berikut 5 baris pertama data:")
    print(df.head())

    # 2. Buat koneksi ke database PostgreSQL menggunakan SQLAlchemy
    # Format URL koneksi: postgresql://user:password@host:port/dbname
    connection_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_url)
    print("\nKoneksi ke database PostgreSQL berhasil dibuat.")

    # 3. Masukkan data dari DataFrame ke tabel PostgreSQL
    # if_exists='replace' akan menghapus tabel lama jika sudah ada dan membuat yang baru
    # if_exists='append' akan menambahkan data jika tabel sudah ada
    print(f"Memasukkan data ke dalam tabel '{table_name}'...")
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print(f"Sukses! Data dari {csv_file_path} telah dimasukkan ke tabel '{table_name}' di database '{db_name}'.")

except FileNotFoundError as e:
    print(e)
except Exception as e:
    print(f"Terjadi error: {e}")