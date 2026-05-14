# Kasly

## Anggota Kelompok :
* Kamrun Syah Syahidu | F1D02410011
* Nisa Aulia Kirani   | F1D02410131
* Oktora Rizka Arifin | F1D02410145

## Deskripsi
Kasly adalah aplikasi berbasis web sebagai pencatatan dan memonitoring keuangan dalam UMKM. Kasly juga dapat digunakan untuk manajemen stok barang, mengeloal utang/piutang dan pelaporan laba rugi.

## 🚀 Fitur Aplikasi KASLY

* 🔐 Multi-Role Login (Owner, Admin, Kasir)
Pengguna dapat masuk ke sistem sesuai hak aksesnya.
    * Owner: Mengelola hak akses dan melihat laporan laba rugi.  
    * Admin: Melakukan update stok dan menerima barang dari supplier.  
    * Kasir: Melakukan input transaksi penjualan secara cepat.  

* 📦 Manajemen Inventaris (Stok FIFO)
    * Fitur untuk memantau ketersediaan stok barang.

* 💳 Point of Sale
    * Memilih produk, menghitung total otomatis, dan cetak struk
    * Mendukung pembayaran tunai maupun nontunai (transfer)

* 📝 Manajemen Utang & Piutang
    * Mencatat transaksi yang belum lunas dengan batas jatuh tempo.
    * Mencatat kewajiban pembayaran kepada supplier atas pembelian stok

* 📊 Dashboard & Laporan Keuangan
    * Perhitungan otomatis omzet dikurangi harga beli.
    * Menampilkan data produk.
    * Ringkasan pendapatan harian yang bisa dipantau langsung oleh Owner

## Team, roles/responsibilities of each member
Kamrun :  Frontend Developer   
* Membuat tampilan antarmuka website dan mengembangkan halaman user                 
Oktora : Backend Developer  
* Mengembangkan sistem backend, serta integrasi database                       
Nisa  : System Analyst & Frontend Developer 
* Melakukan analisis kebutuhan sistem serta membantu pengembangan tampilan frontend


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
* Styling        : Tailwind CSS v4, Flowbite, Font Awesome 6
* Database       : MySQL
* Build Tool     : Tailwind CLI (`npm run build` / `npm run dev`)
* Font           : Plus Jakarta Sans (Google Fonts)
* Package Manager| npm

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
Tabel yang digunakan:
user → Data akun pengguna dan role
produk → Master data produk dan stok
penjualan → Transaksi penjualan
pembelian → Transaksi pembelian / restok
piutang → Data piutang pelanggan

Detail Kolom Utama:
1. Table user
id_user (Primary Key)
id_umkm
nama_lengkap
username
password (hashed)
role (Owner, Admin, Kasir)

2. Table produk
id_produk (Primary Key)
nama_produk
kategori
sisa_stok
harga_jual
harga_beli

3. Table penjualan
tanggal_transaksi
total_harga
metode_pembayaran
status_bayar

4. Table pembelian
tanggal
total_biaya

5. Table piutang
id_piutang
sisa_tagihan
status

