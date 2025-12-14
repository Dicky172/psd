import pandas as pd
from sqlalchemy import create_engine

# --- Konfigurasi koneksi PostgreSQL ---
USER = "postgres"       # ganti dengan username PostgreSQL kamu
PASSWORD = "123456789"  # ganti dengan password PostgreSQL kamu
HOST = "localhost"      # biasanya localhost
PORT = "5432"           # port default PostgreSQL
DATABASE = "bank_postgree"    # nama database yang kamu buat di pgAdmin

# --- Nama tabel yang ingin dibuat di PostgreSQL ---
TABLE_NAME = "bank_postgree" # Nama tabel diubah agar lebih konsisten

# --- Path ke file CSV ---
CSV_FILE_PATH = "bank-full.csv"  # ganti sesuai lokasi file CSV kamu

# --- Buat koneksi ke PostgreSQL ---
engine = create_engine(f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

# --- Baca CSV dengan pandas ---
try:
    # --- PERUBAHAN DI SINI: Ditambahkan sep=';' ---
    df = pd.read_csv(CSV_FILE_PATH, sep=';')
    print("✅ CSV berhasil dibaca dengan pemisah (;) yang benar!")
except FileNotFoundError:
    print("❌ File CSV tidak ditemukan. Pastikan path-nya benar.")
    exit()

# --- Upload data ke PostgreSQL ---
try:
    df.to_sql(TABLE_NAME, engine, if_exists='replace', index=False)
    print(f"✅ Data berhasil diupload ke tabel '{TABLE_NAME}' di database '{DATABASE}'.")
except Exception as e:
    print(f"❌ Terjadi kesalahan saat upload: {e}")