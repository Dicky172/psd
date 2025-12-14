import pandas as pd
from sqlalchemy import create_engine

# --- KONFIGURASI ---
db_user = 'postgres'
db_password = '123456789' # Password diperbarui sesuai input Anda
db_host = 'localhost'
db_port = '5432'          # Port diperbarui sesuai input Anda
db_name = 'bank_postgree'
# Nama tabel diubah agar lebih deskriptif
table_name = 'bank_postgree' 
csv_file_path = 'bank-full.csv'
# ----------------------------------------------------

# 1. Membaca seluruh data dari file CSV
try:
    df_full = pd.read_csv(csv_file_path)
    print(f"✅ Berhasil membaca file '{csv_file_path}'.")

except FileNotFoundError:
    print(f"❌ Error: File tidak ditemukan di path '{csv_file_path}'.")
    exit()

# 2. MEMISAHKAN DAN MENGGABUNGKAN DATA
print("Memfilter data...")

# 2a. Ambil 15 data pertama dari kelas 'Iris-setosa'
df_setosa_15 = df_full[df_full['Class'] == 'Iris-setosa'].head(15)
print(f"-> Ditemukan {len(df_setosa_15)} baris data untuk 'Iris-setosa'.")

# 2b. Ambil SEMUA data dari kelas LAINNYA
df_others = df_full[df_full['Class'] != 'Iris-setosa']
print(f"-> Ditemukan {len(df_others)} baris data untuk kelas selain 'Iris-setosa'.")

# 2c. Gabungkan kedua DataFrame tersebut menjadi satu
df_to_upload = pd.concat([df_setosa_15, df_others], ignore_index=True)

# Menampilkan informasi data yang akan diunggah
print("-" * 50)
print(f"Total data yang akan diunggah: {len(df_to_upload)} baris.")
print("\nVerifikasi jumlah data per kelas yang akan diunggah:")
print(df_to_upload['Class'].value_counts())
print("-" * 50)


# 3. Membuat string koneksi (connection string)
db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

# 4. Membuat engine koneksi dan mengunggah data yang sudah difilter
engine = None
try:
    print("Mencoba terhubung ke database PostgreSQL...")
    engine = create_engine(db_url)
    
    print(f"Mengunggah data ke tabel '{table_name}'...")
    
    # Mengunggah DataFrame gabungan (df_to_upload)
    df_to_upload.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print(f"\n🎉 SUKSES! Data telah diunggah ke tabel '{table_name}'.")

except Exception as e:
    print(f"\n❌ GAGAL terhubung atau mengupload data: {e}")

finally:
    if engine is not None:
        engine.dispose()
        print("Koneksi ke database telah ditutup.")