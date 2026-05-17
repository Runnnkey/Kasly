# Kasly

## Deskripsi
Kasly adalah aplikasi berbasis web sebagai pencatatan dan memonitoring keuangan dalam UMKM. Kasly juga dapat digunakan untuk manajemen stok barang, mengeloal utang/
piutang dan pelaporan laba rugi.

## Team, roles/responsibilities of each member
* __Kamrun Syah Syahidu__ | __F1D02410011__ | Frontend Developer |   
Membuat tampilan antarmuka website dan mengembangkan halaman user

* __Nisa Aulia Kirani__   | __F1D02410131__ | System Analyst & Frontend Developer |
Melakukan analisis kebutuhan sistem serta membantu pengembangan tampilan frontend

* __Oktora Rizka Arifin__ | __F1D02410145__ | Backend Developer & Database |
Mengembangkan sistem backend, serta integrasi database                       

## Users / Actors
* Owner 
  Akses penuh sistem, melihat laporan keuangan lengkap, manajemen user, dan analitik bisnis.
* Admin  
  Mengelola produk, stok barang, serta restok dari supplier.
* Kasir
  Melakukan transaksi penjualan dan input data transaksi harian.

###  Website Features & Menu (Sitemap)

Menu Utama (Sidebar Navigation)

* Dashboard (`index.php`)  
  Ringkasan saldo, greeting user, dan quick access ke fitur lain.

* Transaksi (`transaksi.php`)  
  Riwayat transaksi, total kas, piutang aktif, input penjualan & pembelian.

* Produk (`produk.php`)  
  Manajemen produk, stok barang, health bar stok, edit dan hapus produk.

* Utang & Piutang (`utangPiutang.php`)  
  Kelola piutang pelanggan dan utang ke supplier.

* Laporan (`laporan.php`)  
  Omzet, laba bersih, tren penjualan, produk terlaris, dan inventory alert.

* Pengaturan (`pengaturan.php`)  
  Profil usaha, pengaturan struk, ganti password, dan backup data.

* Keluar (`logout.php`)  
  Logout dari sistem.

* Autentikasi 
  * `login.php`
  * `register.php`


## Tech Stack

* Backend        : PHP Native + MySQLi
* Frontend       : HTML5, JavaScript Vanilla
* Styling        : Tailwind CSS v4, Font Awesome 6
* Database       : MySQL
* Build Tool     : Tailwind CLI (`npm run build` / `npm run dev`)
* Font           : Plus Jakarta Sans (Google Fonts)
* Package Manager: npm

**Dependencies** (package.json):
* tailwindcss
* @tailwindcss/cli
* flowbite

## DBMS Configuration & Table Specification

## Database Configuration (`koneksi.php`)

```php
$host = "localhost";
$user = "root";
$pass = "";
$db   = "kasly";

$conn = mysqli_connect($host, $user, $pass, $db);

Nama Database: kasly
Environment: Local (XAMPP / Laragon)

Table Specification
ms_umkm → Data profil UMKM/unit usaha.
user → Data akun pengguna, role, dan relasi ke UMKM.
produk → Master data produk, kategori, harga, dan stok.
stok_masuk → Log pencatatan stok produk yang masuk.
penjualan → Header transaksi penjualan kepada pelanggan.
penjualan_detail → Rincian item produk dalam setiap transaksi penjualan.
pembelian → Transaksi pembelian barang/stok ke supplier.
supplier → Data vendor atau penyedia barang.
pelanggan → Data entitas pembeli/customer.
piutang → Catatan tagihan penjualan yang belum lunas.
utang → Catatan kewajiban pembayaran kepada supplier.
pembayaran_utang → Log riwayat pembayaran cicilan utang.

Detail Kolom Utama:
1. Table ms_umkm
id_umkm (Primary Key)
nama_usaha
bidang_usaha
alamat
no_telepon

2. Table user
id_user (Primary Key)
id_umkm (Foreign Key)
username
password
role (Owner, Admin, Kasir)
nama_lengkap

3. Table produk
id_produk (Primary Key)
id_umkm (Foreign Key)
nama_produk
kategori
harga_jual
harga_beli
sisa_stok

4. Table penjualan
id_penjualan (Primary Key)
id_user (Foreign Key - Kasir yang bertugas)
tanggal_transaksi
total_harga
metode_pembayaran
status_bayar

5. Table penjualan_detail
id_detail (Primary Key)
id_penjualan (Foreign Key)
id_produk (Foreign Key)
kuantitas
subtotal

6. Table pembelian
id_pembelian (Primary Key)
id_supplier (Foreign Key)
total_biaya
tanggal

7. Table piutang
id_piutang (Primary Key)
id_penjualan (Foreign Key)
id_pelanggan (Foreign Key)
sisa_tagihan
jatuh_tempo
status

8. Table utang
id_utang (Primary Key)
id_pembelian (Foreign Key)
id_supplier (Foreign Key)
total_utang
sisa_utang
status

