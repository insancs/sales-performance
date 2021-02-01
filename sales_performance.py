import pandas as pd
import matplotlib.pyplot as plt

# [1] Load dataset
retail_data1 = pd.read_csv("E:/DQLab/Dataset/retail/retail_data_from_1_until_3.csv", low_memory=False)
retail_data2 = pd.read_csv("E:/DQLab/Dataset/retail/retail_data_from_4_until_6.csv", low_memory=False)
retail_data3 = pd.read_csv("E:/DQLab/Dataset/retail/retail_data_from_7_until_9.csv", low_memory=False)
retail_data4 = pd.read_csv("E:/DQLab/Dataset/retail/retail_data_from_10_until_12.csv", low_memory=False)
print(retail_data1.head(), retail_data2.head(), retail_data3.head(), retail_data4.head())

# [2] Pengcekan Data
print("[2] PENGECEKAN DATA")

# Cek list kolom tiap data apakah tiap data mempunyai kolom yang sama
print("Kolom retail_data1 : %s" % retail_data1.columns)
print("Kolom retail_data2 : %s" % retail_data2.columns)
print("Kolom retail_data3 : %s" % retail_data3.columns)
print("Kolom retail_data4 : %s" % retail_data4.columns)

# Gabungkan tiap data menjadi satu
retail_table = pd.concat([retail_data1, retail_data2, retail_data3, retail_data4])

# Cek info data yg telah digabungkan
print("\nInfo data yang telah digabungkan:\n", retail_table.info())

# Cek statistik deskriptif data
print("\nStatistik Data :\n", retail_table.describe())

# [3] Transformasi Data
print("\n[3] TRANSFORMASI DATA")
# Memastikan data yang memiliki item_price < 0 atau total_price < 0
cek = retail_table.loc[(retail_table['item_price'] < 0) | (retail_table['total_price'] < 0)]
print("item_price < 0 | total_price < 0:\n", cek)

# Jika tidak masuk akal datanya dapat dibuang
if cek.shape[0] != 0:
    retail_table = retail_table.loc[(retail_table['item_price'] > 0) & (retail_table['total_price'] > 0)]

# Cek apakah masih ada order_id yang bernilai undefined dan delete row tersebut
cek = retail_table.loc[retail_table['order_id'] == 'undefined']
print("\norder_id yang bernilai undefined\n", cek)

# Jika ada buang data tersebut
if cek.shape[0] != 0:
    retail_table = retail_table.loc[retail_table['order_id'] != 'undefined']

cek1 = retail_table.loc[retail_table['order_id'] == 'undefined']
print("\norder_id yang bernilai undefined\n", cek1)

# Transformasi order_id dan order_date menjadi int64 dan datetime
retail_table['order_id'] = retail_table['order_id'].astype('int64')
retail_table['order_date'] = pd.to_datetime(retail_table['order_date'])

# Cek info dataframe
print("\nType data :\n", retail_table.dtypes)

# Cek statistik deskriptif kembali, untuk memastikan
print("\nStatistik data :\n", retail_table.describe())

# [4] Filter hanya 5 province terbesar di pulau Jawa
print("\n[4] FILTER PROVINCE TERBESAR DI PULAU JAWA")
# Buat list province yang akan diambil
jawa = ['DKI Jakarta', 'Jawa Barat', 'Jawa Tengah', 'Jawa Timur', 'Yogyakarta']
retail_table = retail_table.loc[retail_table['province'].isin(jawa)]

# Pastikan kolom province isinya sama dengan variabel jawa
print("Province\n", retail_table['province'].unique())

# [5] Kelompokkan sesuai dengan order_date dan province kemudian aggregation
groupby_city_province = retail_table.groupby(['order_date', 'province']).agg({
    'order_id': 'nunique',
    'customer_id': 'nunique',
    'product_id': 'nunique',
    'brand': 'nunique',
    'total_price': sum
})
print("\n[5] Data Hasil groupby\n", groupby_city_province.head())

# Ubah nama kolomnya menjadi 'order','customer','product','brand','GMV'
groupby_city_province.columns = ['order', 'customer', 'product', 'brand', 'GMV']
print("\nData Hasil perubahan nama kolom:\n", groupby_city_province.head())

# [6] Unstack untuk mendapatkan order_date dibagian baris dan province di bagian kolom
retail_table_unstack = groupby_city_province.unstack(level='province').fillna(0)
print("[6] Data Unstack :\n", retail_table_unstack.head())

# [7] Slicing data untuk masing-masing measurement kolom, misal kolom order
idx = pd.IndexSlice
by_order = retail_table_unstack.loc[:, idx['order']]
print("\n[7] Slicing by order :\n", by_order.head())

# [8] Lakukan resampling pada data tersebut untuk dilakukan perhitungan rata-rata bulanan
by_order_monthly = by_order.resample('M').mean()
print("\n[8] Resampling by order bulanan :\n", by_order_monthly.head())

# [9]. Plot untuk hasil pada langkah #[8]
by_order_monthly.plot(
    figsize=(8, 5),
    title='Average Daily order Size in Month View for all Province'
)
plt.xlabel('month')
plt.ylabel('avg order size')
plt.show()

# [10] Buat visualisasi untuk tabel yang lain menggunakan fungsi for
# Create figure canvas dan axes for 5 line plots
fig, axes = plt.subplots(5, 1, figsize=(8, 25))

# Slicing index
for i, measurement in enumerate(groupby_city_province.columns):
    # Slicing data untuk masing-masing measurement
    by_measurement = retail_table_unstack.loc[:, idx[measurement]]
    # Lakukan resampling pada data tersebut untuk dilakukan perhitungan rata-rata bulanan
    by_measurement_monthly = by_measurement.resample('M').mean()
    # Plot by_measurement_monthly
    by_measurement_monthly.plot(
        figsize=(15, 10),
        title='Average Daily' + measurement + 'Size in Month View for all Province',
        ax=axes[i]
    )
    axes[i].set_ylabel('avg' + measurement + 'size')
    axes[i].set_xlabel('month')

# Adjust the layout and show plot
plt.tight_layout()
plt.show()
