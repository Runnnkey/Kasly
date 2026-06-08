<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

$user_id = $_SESSION['user_id'];

$queryUser  = "SELECT id_user, id_umkm, nama_lengkap, role FROM user WHERE id_user = '$user_id'";
$resultUser = mysqli_query($conn, $queryUser);

if (mysqli_num_rows($resultUser) === 0) {
    session_destroy();
    header("Location: login.php");
    exit();
}

$rowUser = mysqli_fetch_assoc($resultUser);
$inisial = strtoupper(substr($rowUser['nama_lengkap'], 0, 1));
$id_umkm = $rowUser['id_umkm'];

$query = "(SELECT tanggal_transaksi as tgl, 'Penjualan' as tipe, total_harga as nominal, metode_pembayaran as metode, status_bayar as status 
           FROM penjualan 
           WHERE id_umkm = '$id_umkm') 
          UNION ALL
          (SELECT tanggal as tgl, 'Pembelian' as tipe, total_biaya as nominal, 'TUNAI' as metode, 'Selesai' as status 
           FROM pembelian 
           WHERE id_umkm = '$id_umkm') 
          ORDER BY tgl DESC";

$result = mysqli_query($conn, $query);

$query_kas = "SELECT SUM(total_harga) as total_kas 
              FROM penjualan 
              WHERE id_umkm = '$id_umkm' 
              AND status_bayar = 'Lunas' 
              AND metode_pembayaran = 'Tunai'";
$res_kas = mysqli_query($conn, $query_kas);
$data_kas = mysqli_fetch_assoc($res_kas);
$total_kas = $data_kas['total_kas'] ?? 0;

$query_piutang = "SELECT SUM(p.sisa_tagihan) as total_piutang, COUNT(p.id_piutang) as jml_transaksi 
                  FROM piutang p
                  INNER JOIN penjualan pj ON p.id_penjualan = pj.id_penjualan
                  WHERE pj.id_umkm = '$id_umkm' 
                  AND p.status = 'Belum Lunas'";
$res_piutang = mysqli_query($conn, $query_piutang);
$data_piutang = mysqli_fetch_assoc($res_piutang);
$total_piutang = $data_piutang['total_piutang'] ?? 0;
$jml_piutang = $data_piutang['jml_transaksi'] ?? 0;

// Cek status notifikasi dari URL
$status = $_GET['status'] ?? '';
?>

<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kasly - Transaksi</title>
    <link rel="stylesheet" href="dist/output.css">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;700&display=swap');

        body {
            font-family: 'Plus Jakarta Sans', sans-serif;
        }

        /* Fix: pastikan backdrop modal menutupi seluruh halaman termasuk area scroll */
        #modalPenjualan,
        #modalPembelian {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0, 0, 0, 0.5);
            z-index: 50;
        }

        /* Modal tambah pelanggan & supplier harus di atas modal utama */
        #modalTambahPelanggan,
        #modalTambahSupplier {
            z-index: 9999 !important;
        }
    </style>
</head>

<body class="bg-slate-50/40">

    <!-- ==================== NOTIFIKASI STATUS ==================== -->
    <?php if ($status == 'penjualan_sukses'): ?>
        <div id="notif-banner" class="p-4 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-2xl text-xs font-semibold flex items-center gap-2 mx-4 mt-4">
            <i class="fa-solid fa-circle-check text-base"></i> Transaksi penjualan berhasil disimpan!
        </div>
    <?php elseif ($status == 'penjualan_gagal'): ?>
        <div id="notif-banner" class="p-4 bg-rose-50 border border-rose-200 text-rose-700 rounded-2xl text-xs font-semibold flex items-center gap-2 mx-4 mt-4">
            <i class="fa-solid fa-circle-exclamation text-base"></i> Gagal menyimpan transaksi penjualan!
        </div>
    <?php elseif ($status == 'pembelian_sukses'): ?>
        <div id="notif-banner" class="p-4 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-2xl text-xs font-semibold flex items-center gap-2 mx-4 mt-4">
            <i class="fa-solid fa-circle-check text-base"></i> Pembelian stok berhasil dicatat!
        </div>
    <?php elseif ($status == 'pembelian_gagal'): ?>
        <div id="notif-banner" class="p-4 bg-rose-50 border border-rose-200 text-rose-700 rounded-2xl text-xs font-semibold flex items-center gap-2 mx-4 mt-4">
            <i class="fa-solid fa-circle-exclamation text-base"></i> Gagal mencatat pembelian stok!
        </div>
    <?php elseif ($status == 'pelanggan_sukses'): ?>
        <div id="notif-banner" class="p-4 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-2xl text-xs font-semibold flex items-center gap-2 mx-4 mt-4">
            <i class="fa-solid fa-circle-check text-base"></i> Pelanggan baru berhasil ditambahkan!
        </div>
    <?php elseif ($status == 'pelanggan_gagal'): ?>
        <div id="notif-banner" class="p-4 bg-rose-50 border border-rose-200 text-rose-700 rounded-2xl text-xs font-semibold flex items-center gap-2 mx-4 mt-4">
            <i class="fa-solid fa-circle-exclamation text-base"></i> Gagal menambahkan pelanggan baru!
        </div>
    <?php elseif ($status == 'supplier_sukses'): ?>
        <div id="notif-banner" class="p-4 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-2xl text-xs font-semibold flex items-center gap-2 mx-4 mt-4">
            <i class="fa-solid fa-circle-check text-base"></i> Supplier baru berhasil ditambahkan!
        </div>
    <?php endif; ?>

    <nav class="flex items-center justify-between px-8 py-4 bg-white shadow-sm sticky top-0 z-50">
        <div class="flex items-center gap-3">
            <button type="button" id="menuBtn" class="p-2 text-slate-600 hover:bg-slate-100 rounded-lg">
                <i class="fa-solid fa-bars text-lg"></i>
            </button>
            <div class="flex items-center gap-2">
                <img src="Assets/LogoBaru.png" alt="Kasly Logo" class="w-12 h-12 object-contain">
            </div>
        </div>
        <div class="flex items-center gap-4">
            <button class="p-2 text-slate-500 hover:bg-slate-100 rounded-full transition">
                <i class="fa-regular fa-bell"></i>
            </button>
            <div class="w-10 h-10 rounded-full bg-indigo-100 border border-indigo-200 flex items-center justify-center text-indigo-600 font-bold">
                <?php echo $inisial; ?>
            </div>
        </div>
    </nav>

    <div id="sidebar" class="fixed top-0 left-0 h-full w-64 bg-white shadow-lg transform -translate-x-full transition-transform duration-300 z-50">
        <nav class="p-6 font-bold text-indigo-600 text-lg border-b flex justify-between items-center">
            Menu
            <div class="flex items-center gap-3">
                <button id="menuBtn2" class="p-2 text-slate-600 hover:bg-slate-100 rounded-lg">
                    <i class="fa-solid fa-bars text-lg"></i>
                </button>
            </div>
        </nav>
        <ul class="p-4 space-y-3">
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="index.php" class="block p-3 w-full h-full">Dashboard</a>
            </li>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="transaksi.php" class="block p-3 w-full h-full">Transaksi</a>
            </li>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="produk.php" class="block p-3 w-full h-full">Produk</a>
            </li>
            <?php if ($rowUser['role'] !== 'Kasir'): ?>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="utangPiutang.php" class="block p-3 w-full h-full">Utang & Piutang</a>
            </li>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="laporan.php" class="block p-3 w-full h-full">Laporan</a>
            </li>
            <?php endif; ?>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="pengaturan.php" class="block p-3 w-full h-full">Pengaturan</a>
            </li>
            <li class="hover:bg-red-50 text-red-600 rounded-lg cursor-pointer transition-colors">
                <a href="logout.php" class="block p-3 w-full h-full flex items-center gap-2">
                    <i class="fa-solid fa-right-from-bracket"></i>
                    <span>Keluar</span>
                </a>
            </li>
        </ul>
    </div>

    <section id="transaksi-page" class="space-y-8 p-4 md:p-6">

        <!-- HEADER SECTION (Mirip dengan produk.php) -->
        <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
            <div class="flex flex-col md:flex-row md:items-center justify-between gap-6">
                <div class="flex items-center gap-4">
                    <div class="w-14 h-14 bg-indigo-100 text-indigo-600 rounded-2xl flex items-center justify-center shadow-inner">
                        <i class="fa-solid fa-chart-line text-2xl"></i>
                    </div>
                    <div>
                        <h2 class="text-2xl font-black text-slate-800 tracking-tight">Catatan Kas & Transaksi</h2>
                        <p class="text-sm text-slate-500 font-medium">Kelola semua arus kas masuk dan keluar di sini.</p>
                    </div>
                </div>
                <div class="flex flex-wrap gap-2">
                    <button class="bg-rose-500 text-white px-5 py-2.5 rounded-xl text-xs font-bold hover:bg-rose-600 hover:scale-[1.02] transition-all flex items-center gap-2 shadow-md shadow-rose-100">
                        <i class="fa-solid fa-receipt"></i> Catat Pembelian
                    </button>
                    <button class="bg-indigo-600 text-white px-5 py-2.5 rounded-xl text-xs font-bold hover:bg-indigo-700 hover:scale-[1.02] transition-all flex items-center gap-2 shadow-md shadow-indigo-100">
                        <i class="fa-solid fa-cart-plus"></i> Input Penjualan
                    </button>
                </div>
            </div>
        </div>

        <!-- CARDS (3 KOLOM) -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div class="bg-gradient-to-br from-indigo-600 to-violet-700 p-6 rounded-2xl text-white shadow-lg shadow-indigo-100 relative overflow-hidden">
                <div class="absolute top-0 right-0 -mr-8 -mt-8 w-32 h-32 bg-white opacity-10 rounded-full"></div>
                <div class="relative z-10">
                    <p class="text-indigo-100 text-xs font-bold uppercase tracking-widest mb-1">Total Kas di Tangan</p>
                    <h3 class="text-2xl font-black">Rp <?= number_format($total_kas, 0, ',', '.') ?></h3>
                    <div class="mt-4 flex items-center gap-2 text-[10px] bg-white/20 w-fit px-2 py-1 rounded-md">
                        <i class="fa-solid fa-clock-rotate-left"></i> Real-time Database
                    </div>
                </div>
            </div>

            <div class="bg-white border border-slate-200 p-6 rounded-2xl shadow-sm group hover:border-amber-200 transition-colors flex flex-col justify-between">
                <div class="flex justify-between items-start">
                    <p class="text-slate-500 text-xs font-bold uppercase tracking-wider">Piutang Aktif</p>
                    <span class="bg-amber-100 text-amber-600 text-[10px] px-2 py-1 rounded-full font-bold">
                        <?= $jml_piutang ?> Transaksi
                    </span>
                </div>
                <h3 class="text-2xl font-black text-slate-800 mt-2">Rp <?= number_format($total_piutang, 0, ',', '.') ?></h3>
                <a href="utangPiutang.php" class="text-xs text-slate-400 mt-2 hover:text-indigo-600 cursor-pointer italic underline inline-block">
                    Lihat semua piutang →
                </a>
            </div>

            <div class="bg-white border border-slate-200 p-6 rounded-2xl shadow-sm group hover:border-indigo-200 transition-colors flex items-center gap-4">
                <div class="w-12 h-12 bg-slate-100 text-slate-400 rounded-2xl flex items-center justify-center group-hover:bg-indigo-50 group-hover:text-indigo-600 transition-all">
                    <i class="fa-solid fa-calendar-check text-xl"></i>
                </div>
                <div>
                    <p class="text-slate-500 text-[10px] font-bold uppercase tracking-wider">Bayar Rutin</p>
                    <p class="text-sm font-bold text-slate-800 italic">2 Tagihan besok</p>
                    <button class="text-[10px] text-indigo-600 font-bold mt-1 uppercase tracking-tighter hover:tracking-normal transition-all">Kelola Jadwal</button>
                </div>
            </div>
        </div>

        <!-- FILTER BAR (Mirip dengan produk.php) -->
        <div class="bg-white p-4 rounded-2xl border border-slate-200 flex flex-wrap gap-4 items-center shadow-sm">
            <div class="flex items-center gap-2 px-3 py-2 bg-slate-50 rounded-xl border border-slate-100">
                <i class="fa-solid fa-calendar text-slate-400 text-xs"></i>
                <select class="bg-transparent text-xs font-bold text-slate-600 outline-none">
                    <option>Hari Ini</option>
                    <option>Minggu Ini</option>
                    <option>Bulan Ini</option>
                    <option>Custom Range</option>
                </select>
            </div>
            <div class="flex items-center gap-2 px-3 py-2 bg-slate-50 rounded-xl border border-slate-100">
                <i class="fa-solid fa-filter text-slate-400 text-xs"></i>
                <select class="bg-transparent text-xs font-bold text-slate-600 outline-none">
                    <option>Semua Kategori</option>
                    <option>Penjualan</option>
                    <option>Biaya Operasional</option>
                    <option>Stok Barang</option>
                </select>
            </div>
            <div class="flex items-center gap-2 px-3 py-2 bg-slate-50 rounded-xl border border-slate-100">
                <i class="fa-solid fa-credit-card text-slate-400 text-xs"></i>
                <select class="bg-transparent text-xs font-bold text-slate-600 outline-none">
                    <option>Semua Metode</option>
                    <option>Tunai</option>
                    <option>Transfer Bank</option>
                    <option>QRIS</option>
                </select>
            </div>
            <div class="ml-auto relative w-full md:w-auto">
                <i class="fa-solid fa-magnifying-glass absolute left-3 top-1/2 -translate-y-1/2 text-slate-300 text-xs"></i>
                <input type="text" placeholder="Cari transaksi..." class="pl-9 pr-4 py-2 bg-slate-50 border border-slate-200 rounded-xl text-xs w-full focus:ring-2 focus:ring-indigo-500 outline-none">
            </div>
        </div>

        <!-- TABEL TRANSAKSI (Mirip gaya produk.php) -->
        <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
            <div class="p-6 border-b border-slate-100 flex justify-between items-center bg-slate-50/50">
                <h3 class="font-bold text-slate-800 flex items-center gap-2 uppercase text-xs tracking-widest">
                    <i class="fa-solid fa-clock-rotate-left text-indigo-500"></i> Riwayat Transaksi
                </h3>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse">
                    <thead class="bg-slate-50">
                        <tr>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Waktu & Tipe</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Keterangan</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Metode</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider text-center">Status</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider text-right">Nominal</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100">
                        <?php while ($row = mysqli_fetch_assoc($result)) :
                            $isPenjualan = ($row['tipe'] == 'Penjualan');
                            $bgIcon = $isPenjualan ? 'bg-emerald-100 text-emerald-600' : 'bg-rose-100 text-rose-600';
                            $icon = $isPenjualan ? 'fa-arrow-down' : 'fa-arrow-up';
                            $colorNominal = $isPenjualan ? 'text-emerald-600' : 'text-rose-600';
                            $prefix = $isPenjualan ? '+ ' : '- ';
                        ?>
                            <tr class="hover:bg-slate-50 transition group">
                                <td class="p-4">
                                    <div class="flex items-center gap-3">
                                        <div class="w-8 h-8 <?= $bgIcon ?> rounded-lg flex items-center justify-center">
                                            <i class="fa-solid <?= $icon ?> text-xs"></i>
                                        </div>
                                        <div>
                                            <p class="text-xs font-bold text-slate-700"><?= $row['tipe'] ?></p>
                                            <p class="text-[10px] text-slate-400"><?= date('d M Y', strtotime($row['tgl'])) ?></p>
                                        </div>
                                    </div>
                                </td>
                                <td class="p-4">
                                    <p class="text-sm font-medium text-slate-800"><?= $isPenjualan ? "Transaksi Penjualan" : "Restok Barang" ?></p>
                                </td>
                                <td class="p-4">
                                    <span class="text-[10px] font-bold bg-blue-50 text-blue-600 px-2 py-1 rounded-md uppercase"><?= $row['metode'] ?></span>
                                </td>
                                <td class="p-4">
                                    <?php if ($row['status'] == 'Lunas' || $row['status'] == 'Selesai') : ?>
                                        <span class="text-[10px] font-bold text-emerald-600 flex items-center gap-1">
                                            <i class="fa-solid fa-circle text-[6px]"></i> <?= $row['status'] ?>
                                        </span>
                                    <?php else : ?>
                                        <span class="text-[10px] font-bold bg-amber-50 text-amber-600 px-2 py-1 rounded-md">Menunggu Bayar</span>
                                    <?php endif; ?>
                                </td>
                                <td class="p-4 text-right">
                                    <p class="text-sm font-black <?= $colorNominal ?>"><?= $prefix ?>Rp <?= number_format($row['nominal'], 0, ',', '.') ?></p>
                                </td>
                            </tr>
                        <?php endwhile; ?>
                    </tbody>
                </table>
            </div>
            <div class="p-4 bg-slate-50/50 border-t border-slate-100 flex justify-between items-center text-[10px] font-bold text-slate-400 uppercase tracking-wider">
                <div class="flex gap-2">
                    <button class="px-3 py-1 rounded bg-white border border-slate-200 hover:bg-slate-50 transition-all">Prev</button>
                    <button class="px-3 py-1 rounded bg-white border border-slate-200 hover:bg-slate-50 transition-all">Next</button>
                </div>
            </div>
        </div>

        <!-- ==================== POP UP PENJUALAN ==================== -->
        <div id="modalPenjualan" class="hidden fixed inset-0 bg-black/50 z-50 flex items-start justify-center py-10 px-4 overflow-y-auto min-h-screen">
            <div class="bg-white w-full max-w-4xl rounded-3xl p-6 relative overflow-y-auto max-h-[90vh]">

                <button id="closePenjualan" class="absolute top-5 right-5 w-10 h-10 rounded-full bg-slate-100 hover:bg-red-100 text-slate-500 hover:text-red-500 transition flex items-center justify-center">
                    <i class="fa-solid fa-xmark text-lg"></i>
                </button>

                <!-- PENCATATAN PENJUALAN -->
                <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
                    <div class="flex items-center gap-3 mb-6 border-b border-slate-100 pb-4">
                        <div class="w-10 h-10 bg-emerald-50 text-emerald-600 rounded-xl flex items-center justify-center">
                            <i class="fa-solid fa-cart-shopping text-lg"></i>
                        </div>
                        <div>
                            <h3 class="text-base font-black text-slate-800 tracking-tight">Transaksi Penjualan</h3>
                            <p class="text-xs text-slate-400 font-medium">Catat penjualan tunai atau kredit ke pelanggan.</p>
                        </div>
                    </div>

                    <form action="proses_penjualan.php" method="POST" class="space-y-4">
                        <input type="hidden" name="id_user" value="<?php echo $user_id; ?>">
                        <input type="hidden" name="id_umkm" value="<?php echo $id_umkm; ?>">

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Tanggal Transaksi</label>
                                <input type="date" name="tanggal_transaksi" value="<?php echo date('Y-m-d'); ?>" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-emerald-500 outline-none text-sm font-medium text-slate-700">
                            </div>
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Kasir</label>
                                <input type="text" value="<?php echo htmlspecialchars($rowUser['nama_lengkap']); ?>" readonly class="w-full p-3 bg-slate-100 border border-slate-200 rounded-xl outline-none text-sm font-medium text-slate-500 cursor-not-allowed">
                            </div>
                        </div>

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Pilih Pelanggan</label>
                                <select name="id_pelanggan" id="id_pelanggan_penjualan" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-emerald-500 outline-none text-sm font-medium text-slate-700 appearance-none">
                                    <option value="">-- Pilih Pelanggan --</option>
                                    <?php
                                    $queryPelanggan = "SELECT id_pelanggan, nama_pelanggan FROM pelanggan ORDER BY nama_pelanggan ASC";
                                    $resultPelanggan = mysqli_query($conn, $queryPelanggan);
                                    while ($pelanggan = mysqli_fetch_assoc($resultPelanggan)) {
                                        echo '<option value="' . $pelanggan['id_pelanggan'] . '">' . htmlspecialchars($pelanggan['nama_pelanggan']) . '</option>';
                                    }
                                    ?>
                                </select>
                            </div>
                            <div class="space-y-2 flex items-end">
                                <button type="button" id="btnTambahPelanggan" class="px-4 py-3 bg-emerald-50 text-emerald-600 rounded-xl text-xs font-bold hover:bg-emerald-100 transition flex items-center gap-2">
                                    <i class="fa-solid fa-user-plus"></i> Tambah Pelanggan Baru
                                </button>
                            </div>
                        </div>

                        <!-- TAMBAH PRODUK -->
                        <div class="border-t border-slate-100 pt-4 mt-2">
                            <label class="text-xs font-black text-slate-500 uppercase block mb-3">Tambah Produk</label>
                            <div class="grid grid-cols-1 sm:grid-cols-4 gap-3">
                                <div class="sm:col-span-2">
                                    <select id="pilih_produk" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-emerald-500 outline-none text-sm font-medium text-slate-700">
                                        <option value="">-- Pilih Produk --</option>
                                        <?php
                                        $queryProduk = "SELECT id_produk, nama_produk, harga_jual, sisa_stok FROM produk WHERE id_umkm = '$id_umkm' AND sisa_stok > 0";
                                        $resultProduk = mysqli_query($conn, $queryProduk);
                                        while ($produk = mysqli_fetch_assoc($resultProduk)) {
                                            echo '<option value="' . $produk['id_produk'] . '" data-harga="' . $produk['harga_jual'] . '" data-stok="' . $produk['sisa_stok'] . '" data-nama="' . htmlspecialchars($produk['nama_produk']) . '">';
                                            echo htmlspecialchars($produk['nama_produk']) . ' - Rp ' . number_format($produk['harga_jual'], 0, ',', '.') . ' (Stok: ' . $produk['sisa_stok'] . ')';
                                            echo '</option>';
                                        }
                                        ?>
                                    </select>
                                </div>
                                <div>
                                    <input type="number" id="qty_produk" placeholder="Kuantitas" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-emerald-500 outline-none text-sm font-medium text-slate-700">
                                </div>
                                <div>
                                    <button type="button" id="btnTambahKeranjang" class="w-full px-4 py-3 bg-emerald-600 text-white rounded-xl text-xs font-bold hover:bg-emerald-700 transition flex items-center justify-center gap-2">
                                        <i class="fa-solid fa-plus"></i> Tambah
                                    </button>
                                </div>
                            </div>
                        </div>

                        <!-- KERANJANG BELANJA -->
                        <div class="space-y-2">
                            <label class="text-xs font-black text-slate-500 uppercase">Keranjang Belanja</label>
                            <div class="bg-slate-50 rounded-xl border border-slate-200 overflow-hidden">
                                <div class="overflow-x-auto">
                                    <table class="w-full text-sm">
                                        <thead class="bg-slate-100">
                                            <tr>
                                                <th class="p-3 text-left text-xs font-black text-slate-500">Produk</th>
                                                <th class="p-3 text-center text-xs font-black text-slate-500">Qty</th>
                                                <th class="p-3 text-right text-xs font-black text-slate-500">Harga</th>
                                                <th class="p-3 text-right text-xs font-black text-slate-500">Subtotal</th>
                                                <th class="p-3 text-center text-xs font-black text-slate-500">Aksi</th>
                                            </tr>
                                        </thead>
                                        <tbody id="keranjang_tbody">
                                            <tr>
                                                <td colspan="5" class="p-6 text-center text-slate-400">Belum ada produk</td>
                                            </tr>
                                        </tbody>
                                        <tfoot class="bg-slate-100 border-t border-slate-200">
                                            <tr>
                                                <td colspan="3" class="p-3 text-right font-black text-slate-700">TOTAL:</td>
                                                <td class="p-3 text-right font-black text-emerald-600" id="total_harga_display">Rp 0</td>
                                                <td></td>
                                            </tr>
                                        </tfoot>
                                    </table>
                                </div>
                            </div>
                            <input type="hidden" name="total_harga" id="total_harga_input" value="0">
                            <input type="hidden" name="detail_transaksi" id="detail_transaksi_input" value="">
                        </div>

                        <!-- METODE PEMBAYARAN -->
                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Metode Pembayaran</label>
                                <div class="flex gap-4">
                                    <label class="flex items-center gap-2 cursor-pointer">
                                        <input type="radio" name="metode_pembayaran" value="Tunai" checked class="w-4 h-4 text-emerald-600">
                                        <span class="text-sm font-medium text-slate-700">Tunai</span>
                                    </label>
                                    <label class="flex items-center gap-2 cursor-pointer">
                                        <input type="radio" name="metode_pembayaran" value="Kredit" id="radio_kredit" class="w-4 h-4 text-emerald-600">
                                        <span class="text-sm font-medium text-slate-700">Kredit (Piutang)</span>
                                    </label>
                                </div>
                            </div>
                            <div class="space-y-2" id="jatuh_tempo_container" style="display: none;">
                                <label class="text-xs font-black text-slate-500 uppercase">Jatuh Tempo</label>
                                <input type="date" name="jatuh_tempo" id="jatuh_tempo" value="<?php echo date('Y-m-d', strtotime('+7 days')); ?>" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-emerald-500 outline-none text-sm font-medium text-slate-700">
                            </div>
                        </div>

                        <div class="flex justify-end pt-2">
                            <button type="submit" name="btn_simpan_penjualan" class="w-full px-6 py-2.5 bg-emerald-600 text-white rounded-xl text-xs font-bold hover:bg-emerald-700 hover:scale-[1.02] transition-all shadow-md shadow-emerald-100 flex items-center justify-center gap-2">
                                <i class="fa-solid fa-floppy-disk"></i> Proses Transaksi
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        </div>

        <!-- ==================== POP UP PEMBELIAN ==================== -->
        <div id="modalPembelian" class="hidden fixed inset-0 bg-black/50 z-50 flex items-start justify-center py-10 px-4 overflow-y-auto min-h-screen">
            <div class="bg-white w-full max-w-4xl rounded-3xl p-6 relative overflow-y-auto max-h-[90vh]">

                <button id="closePembelian" class="absolute top-5 right-5 w-10 h-10 rounded-full bg-slate-100 hover:bg-red-100 text-slate-500 hover:text-red-500 transition flex items-center justify-center">
                    <i class="fa-solid fa-xmark text-lg"></i>
                </button>

                <!-- PENCATATAN PEMBELIAN -->
                <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
                    <div class="flex items-center gap-3 mb-6 border-b border-slate-100 pb-4">
                        <div class="w-10 h-10 bg-purple-50 text-purple-600 rounded-xl flex items-center justify-center">
                            <i class="fa-solid fa-truck text-lg"></i>
                        </div>
                        <div>
                            <h3 class="text-base font-black text-slate-800 tracking-tight">Pembelian dari Supplier</h3>
                            <p class="text-xs text-slate-400 font-medium">Catat pembelian stok barang dari supplier.</p>
                        </div>
                    </div>

                    <form action="proses_pembelian.php" method="POST" class="space-y-4">
                        <input type="hidden" name="id_user" value="<?php echo $user_id; ?>">
                        <input type="hidden" name="id_umkm" value="<?php echo $id_umkm; ?>">

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Tanggal Pembelian</label>
                                <input type="date" name="tanggal_pembelian" value="<?php echo date('Y-m-d'); ?>" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-purple-500 outline-none text-sm font-medium text-slate-700">
                            </div>
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Petugas</label>
                                <input type="text" value="<?php echo htmlspecialchars($rowUser['nama_lengkap']); ?>" readonly class="w-full p-3 bg-slate-100 border border-slate-200 rounded-xl outline-none text-sm font-medium text-slate-500 cursor-not-allowed">
                            </div>
                        </div>

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Pilih Supplier</label>
                                <select name="id_supplier" id="id_supplier_pembelian" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-purple-500 outline-none text-sm font-medium text-slate-700 appearance-none">
                                    <option value="">-- Pilih Supplier --</option>
                                    <?php
                                    $querySupplier = "SELECT id_supplier, nama_supplier FROM supplier ORDER BY nama_supplier ASC";
                                    $resultSupplier = mysqli_query($conn, $querySupplier);
                                    while ($supplier = mysqli_fetch_assoc($resultSupplier)) {
                                        echo '<option value="' . $supplier['id_supplier'] . '">' . htmlspecialchars($supplier['nama_supplier']) . '</option>';
                                    }
                                    ?>
                                </select>
                            </div>
                            <div class="space-y-2 flex items-end">
                                <button type="button" id="btnTambahSupplier" class="px-4 py-3 bg-purple-50 text-purple-600 rounded-xl text-xs font-bold hover:bg-purple-100 transition flex items-center gap-2">
                                    <i class="fa-solid fa-truck-plus"></i> Tambah Supplier Baru
                                </button>
                            </div>
                        </div>

                        <!-- TAMBAH PRODUK -->
                        <div class="border-t border-slate-100 pt-4 mt-2">
                            <label class="text-xs font-black text-slate-500 uppercase block mb-3">Tambah Produk</label>
                            <div class="grid grid-cols-1 sm:grid-cols-4 gap-3">
                                <div class="sm:col-span-2">
                                    <select id="pilih_produk_pembelian" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-purple-500 outline-none text-sm font-medium text-slate-700">
                                        <option value="">-- Pilih Produk --</option>
                                        <?php
                                        $queryProdukAll = "SELECT id_produk, nama_produk, harga_beli FROM produk WHERE id_umkm = '$id_umkm'";
                                        $resultProdukAll = mysqli_query($conn, $queryProdukAll);
                                        while ($produk = mysqli_fetch_assoc($resultProdukAll)) {
                                            echo '<option value="' . $produk['id_produk'] . '" data-harga_beli="' . $produk['harga_beli'] . '" data-nama="' . htmlspecialchars($produk['nama_produk']) . '">';
                                            echo htmlspecialchars($produk['nama_produk']) . ' - Harga Beli: Rp ' . number_format($produk['harga_beli'], 0, ',', '.');
                                            echo '</option>';
                                        }
                                        ?>
                                    </select>
                                </div>
                                <div>
                                    <input type="number" id="qty_produk_pembelian" placeholder="Kuantitas" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-purple-500 outline-none text-sm font-medium text-slate-700">
                                </div>
                                <div>
                                    <input type="number" id="harga_beli_manual" placeholder="Harga Beli (manual)" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-purple-500 outline-none text-sm font-medium text-slate-700">
                                </div>
                                <div>
                                    <button type="button" id="btnTambahKeranjangPembelian" class="w-full px-4 py-3 bg-purple-600 text-white rounded-xl text-xs font-bold hover:bg-purple-700 transition flex items-center justify-center gap-2">
                                        <i class="fa-solid fa-plus"></i> Tambah
                                    </button>
                                </div>
                            </div>
                        </div>

                        <!-- KERANJANG BELANJA -->
                        <div class="space-y-2">
                            <label class="text-xs font-black text-slate-500 uppercase">Keranjang Belanja</label>
                            <div class="bg-slate-50 rounded-xl border border-slate-200 overflow-hidden">
                                <div class="overflow-x-auto">
                                    <table class="w-full text-sm">
                                        <thead class="bg-slate-100">
                                            <tr>
                                                <th class="p-3 text-left text-xs font-black text-slate-500">Produk</th>
                                                <th class="p-3 text-center text-xs font-black text-slate-500">Qty</th>
                                                <th class="p-3 text-right text-xs font-black text-slate-500">Harga Beli</th>
                                                <th class="p-3 text-right text-xs font-black text-slate-500">Subtotal</th>
                                                <th class="p-3 text-center text-xs font-black text-slate-500">Aksi</th>
                                            </tr>
                                        </thead>
                                        <tbody id="keranjang_pembelian_tbody">
                                            <tr>
                                                <td colspan="5" class="p-6 text-center text-slate-400">Belum ada produk</td>
                                            </tr>
                                        </tbody>
                                        <tfoot class="bg-slate-100 border-t border-slate-200">
                                            <tr>
                                                <td colspan="3" class="p-3 text-right font-black text-slate-700">TOTAL:</td>
                                                <td class="p-3 text-right font-black text-purple-600" id="total_biaya_display">Rp 0</td>
                                                <td></td>
                                            </tr>
                                        </tfoot>
                                    </table>
                                </div>
                            </div>
                            <input type="hidden" name="total_biaya" id="total_biaya_input" value="0">
                            <input type="hidden" name="detail_pembelian" id="detail_pembelian_input" value="">
                        </div>

                        <!-- METODE PEMBAYARAN -->
                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Metode Pembayaran</label>
                                <div class="flex gap-4">
                                    <label class="flex items-center gap-2 cursor-pointer">
                                        <input type="radio" name="metode_pembayaran_pembelian" value="Tunai" checked class="w-4 h-4 text-purple-600">
                                        <span class="text-sm font-medium text-slate-700">Tunai</span>
                                    </label>
                                    <label class="flex items-center gap-2 cursor-pointer">
                                        <input type="radio" name="metode_pembayaran_pembelian" value="Kredit" id="radio_kredit_pembelian" class="w-4 h-4 text-purple-600">
                                        <span class="text-sm font-medium text-slate-700">Kredit (Utang)</span>
                                    </label>
                                </div>
                            </div>
                            <div class="space-y-2" id="jatuh_tempo_utang_container" style="display: none;">
                                <label class="text-xs font-black text-slate-500 uppercase">Jatuh Tempo (Utang)</label>
                                <input type="date" name="jatuh_tempo_utang" id="jatuh_tempo_utang" value="<?php echo date('Y-m-d', strtotime('+30 days')); ?>" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-purple-500 outline-none text-sm font-medium text-slate-700">
                            </div>
                        </div>

                        <div class="flex justify-end pt-2">
                            <button type="submit" name="btn_simpan_pembelian" class="w-full px-6 py-2.5 bg-purple-600 text-white rounded-xl text-xs font-bold hover:bg-purple-700 hover:scale-[1.02] transition-all shadow-md shadow-purple-100 flex items-center justify-center gap-2">
                                <i class="fa-solid fa-floppy-disk"></i> Simpan Pembelian
                            </button>
                        </div>
                    </form>
                </div>
            </div>
        </div>

    </section>


    <!-- ==================== MODAL TAMBAH PELANGGAN ==================== -->
    <div id="modalTambahPelanggan" class="hidden fixed inset-0 z-[60] flex items-center justify-center px-4" style="background-color: rgba(0,0,0,0.6);">
        <div class="bg-white w-full max-w-md rounded-2xl p-6 relative shadow-xl">
            <button id="closeTambahPelanggan" class="absolute top-4 right-4 w-9 h-9 rounded-full bg-slate-100 hover:bg-red-100 text-slate-500 hover:text-red-500 transition flex items-center justify-center">
                <i class="fa-solid fa-xmark"></i>
            </button>
            <div class="flex items-center gap-3 mb-5 pb-4 border-b border-slate-100">
                <div class="w-10 h-10 bg-emerald-50 text-emerald-600 rounded-xl flex items-center justify-center">
                    <i class="fa-solid fa-user-plus text-lg"></i>
                </div>
                <div>
                    <h3 class="text-sm font-black text-slate-800">Tambah Pelanggan Baru</h3>
                    <p class="text-xs text-slate-400">Isi data lalu simpan, halaman akan kembali otomatis</p>
                </div>
            </div>
            <form action="proses_tambah_pelanggan.php" method="POST" id="formTambahPelanggan" class="space-y-3">
                <div>
                    <label class="text-xs font-black text-slate-500 uppercase block mb-1">Nama Pelanggan <span class="text-red-500">*</span></label>
                    <input type="text" name="nama_pelanggan" required placeholder="Masukkan nama pelanggan" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-emerald-500 outline-none text-sm font-medium text-slate-800">
                </div>
                <button type="submit" class="w-full py-3 bg-emerald-600 text-white rounded-xl text-sm font-bold hover:bg-emerald-700 transition flex items-center justify-center gap-2">
                    <i class="fa-solid fa-floppy-disk"></i> Simpan Pelanggan
                </button>
            </form>
        </div>
    </div>

    <!-- ==================== MODAL TAMBAH SUPPLIER ==================== -->
    <div id="modalTambahSupplier" class="hidden fixed inset-0 z-[60] flex items-center justify-center px-4" style="background-color: rgba(0,0,0,0.6);">
        <div class="bg-white w-full max-w-md rounded-2xl p-6 relative shadow-xl">
            <button id="closeTambahSupplier" class="absolute top-4 right-4 w-9 h-9 rounded-full bg-slate-100 hover:bg-red-100 text-slate-500 hover:text-red-500 transition flex items-center justify-center">
                <i class="fa-solid fa-xmark"></i>
            </button>
            <div class="flex items-center gap-3 mb-5 pb-4 border-b border-slate-100">
                <div class="w-10 h-10 bg-purple-50 text-purple-600 rounded-xl flex items-center justify-center">
                    <i class="fa-solid fa-truck-plus text-lg"></i>
                </div>
                <div>
                    <h3 class="text-sm font-black text-slate-800">Tambah Supplier Baru</h3>
                    <p class="text-xs text-slate-400">Isi data lalu simpan, halaman akan kembali otomatis</p>
                </div>
            </div>
            <form action="proses_tambah_supplier.php" method="POST" id="formTambahSupplier" class="space-y-3">
                <div>
                    <label class="text-xs font-black text-slate-500 uppercase block mb-1">Nama Supplier <span class="text-red-500">*</span></label>
                    <input type="text" name="nama_supplier" required placeholder="Masukkan nama supplier" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-purple-500 outline-none text-sm font-medium text-slate-800">
                </div>
                <div>
                    <label class="text-xs font-black text-slate-500 uppercase block mb-1">Kontak</label>
                    <input type="text" name="kontak" placeholder="No. HP / Email supplier" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-purple-500 outline-none text-sm font-medium text-slate-800">
                </div>
                <button type="submit" class="w-full py-3 bg-purple-600 text-white rounded-xl text-sm font-bold hover:bg-purple-700 transition flex items-center justify-center gap-2">
                    <i class="fa-solid fa-floppy-disk"></i> Simpan Supplier
                </button>
            </form>
        </div>
    </div>

    <script src="src/js/script.js"></script>
    <script>
        // ==================== MODAL PENJUALAN ====================
        const modalPenjualan = document.getElementById('modalPenjualan');
        const closePenjualan = document.getElementById('closePenjualan');

        const btnInputPenjualan = document.querySelector('.bg-indigo-600.text-white');
        if (btnInputPenjualan && btnInputPenjualan.innerText.includes('Input Penjualan')) {
            btnInputPenjualan.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                if (modalPenjualan) modalPenjualan.classList.remove('hidden');
                document.body.classList.add('overflow-hidden');
            });
        }

        if (closePenjualan) {
            closePenjualan.addEventListener('click', () => {
                modalPenjualan.classList.add('hidden');
                document.body.classList.remove('overflow-hidden');
            });
        }

        // ==================== MODAL PEMBELIAN ====================
        const modalPembelian = document.getElementById('modalPembelian');
        const closePembelian = document.getElementById('closePembelian');

        const btnInputPembelian = document.querySelector('.bg-rose-500.text-white');
        if (btnInputPembelian && btnInputPembelian.innerText.includes('Catat Pembelian')) {
            btnInputPembelian.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();
                if (modalPembelian) modalPembelian.classList.remove('hidden');
                document.body.classList.add('overflow-hidden');
            });
        }

        if (closePembelian) {
            closePembelian.addEventListener('click', () => {
                modalPembelian.classList.add('hidden');
                document.body.classList.remove('overflow-hidden');
            });
        }

        // ==================== TOGGLE JATUH TEMPO ====================
        const radioKredit = document.getElementById('radio_kredit');
        const jatuhTempoContainer = document.getElementById('jatuh_tempo_container');
        if (radioKredit) {
            radioKredit.addEventListener('change', function() {
                jatuhTempoContainer.style.display = this.checked ? 'block' : 'none';
            });
        }

        const radioKreditPembelian = document.getElementById('radio_kredit_pembelian');
        const jatuhTempoUtangContainer = document.getElementById('jatuh_tempo_utang_container');
        if (radioKreditPembelian) {
            radioKreditPembelian.addEventListener('change', function() {
                jatuhTempoUtangContainer.style.display = this.checked ? 'block' : 'none';
            });
        }

        // ==================== KERANJANG ====================
        let keranjang = [];
        let keranjangPembelian = [];

        function updateKeranjangDisplay() {
            const tbody = document.getElementById('keranjang_tbody');
            let total = 0;

            if (!tbody) return;

            if (keranjang.length === 0) {
                tbody.innerHTML = '<tr><td colspan="5" class="p-6 text-center text-slate-400">Belum ada produk</td></tr>';
                document.getElementById('total_harga_display').innerText = 'Rp 0';
                document.getElementById('total_harga_input').value = '0';
                document.getElementById('detail_transaksi_input').value = '';
                return;
            }

            tbody.innerHTML = '';
            keranjang.forEach((item, idx) => {
                const subtotal = item.qty * item.harga;
                total += subtotal;
                tbody.innerHTML += `
                <tr class="border-b border-slate-100">
                    <td class="p-3 text-left text-sm font-medium text-slate-800">${item.nama}</td>
                    <td class="p-3 text-center text-sm">${item.qty}</td>
                    <td class="p-3 text-right text-sm">Rp ${item.harga.toLocaleString('id-ID')}</td>
                    <td class="p-3 text-right text-sm font-semibold text-emerald-600">Rp ${subtotal.toLocaleString('id-ID')}</td>
                    <td class="p-3 text-center"><button type="button" onclick="hapusDariKeranjang(${idx})" class="text-red-500 hover:text-red-700"><i class="fa-solid fa-trash-can"></i></button></td>
                </tr>
            `;
            });

            document.getElementById('total_harga_display').innerText = `Rp ${total.toLocaleString('id-ID')}`;
            document.getElementById('total_harga_input').value = total;
            document.getElementById('detail_transaksi_input').value = JSON.stringify(keranjang);
        }

        window.hapusDariKeranjang = function(idx) {
            keranjang.splice(idx, 1);
            updateKeranjangDisplay();
        };

        document.getElementById('btnTambahKeranjang')?.addEventListener('click', function() {
            const select = document.getElementById('pilih_produk');
            if (!select) return;
            const selectedOption = select.options[select.selectedIndex];
            const id_produk = select.value;
            const nama = selectedOption.dataset.nama;
            const harga = parseInt(selectedOption.dataset.harga);
            const qty = parseInt(document.getElementById('qty_produk').value);

            if (!id_produk || !qty || qty < 1) {
                alert('Pilih produk dan masukkan kuantitas yang valid!');
                return;
            }

            keranjang.push({
                id_produk,
                nama,
                harga,
                qty
            });
            updateKeranjangDisplay();
            document.getElementById('qty_produk').value = '';
            select.value = '';
        });

        function updateKeranjangPembelianDisplay() {
            const tbody = document.getElementById('keranjang_pembelian_tbody');
            let total = 0;

            if (!tbody) return;

            if (keranjangPembelian.length === 0) {
                tbody.innerHTML = '<tr><td colspan="5" class="p-6 text-center text-slate-400">Belum ada produk</td></tr>';
                document.getElementById('total_biaya_display').innerText = 'Rp 0';
                document.getElementById('total_biaya_input').value = '0';
                document.getElementById('detail_pembelian_input').value = '';
                return;
            }

            tbody.innerHTML = '';
            keranjangPembelian.forEach((item, idx) => {
                const subtotal = item.qty * item.harga_beli;
                total += subtotal;
                tbody.innerHTML += `
                <tr class="border-b border-slate-100">
                    <td class="p-3 text-left text-sm font-medium text-slate-800">${item.nama}</td>
                    <td class="p-3 text-center text-sm">${item.qty}</td>
                    <td class="p-3 text-right text-sm">Rp ${item.harga_beli.toLocaleString('id-ID')}</td>
                    <td class="p-3 text-right text-sm font-semibold text-purple-600">Rp ${subtotal.toLocaleString('id-ID')}</td>
                    <td class="p-3 text-center"><button type="button" onclick="hapusDariKeranjangPembelian(${idx})" class="text-red-500 hover:text-red-700"><i class="fa-solid fa-trash-can"></i></button></td>
                </tr>
            `;
            });

            document.getElementById('total_biaya_display').innerText = `Rp ${total.toLocaleString('id-ID')}`;
            document.getElementById('total_biaya_input').value = total;
            document.getElementById('detail_pembelian_input').value = JSON.stringify(keranjangPembelian);
        }

        window.hapusDariKeranjangPembelian = function(idx) {
            keranjangPembelian.splice(idx, 1);
            updateKeranjangPembelianDisplay();
        };

        document.getElementById('btnTambahKeranjangPembelian')?.addEventListener('click', function() {
            const select = document.getElementById('pilih_produk_pembelian');
            if (!select) return;
            const selectedOption = select.options[select.selectedIndex];
            const id_produk = select.value;
            const nama = selectedOption.dataset.nama;
            let harga_beli = parseInt(document.getElementById('harga_beli_manual').value);
            const qty = parseInt(document.getElementById('qty_produk_pembelian').value);

            if (!id_produk || !qty || qty < 1) {
                alert('Pilih produk dan masukkan kuantitas yang valid!');
                return;
            }

            if (isNaN(harga_beli) || harga_beli <= 0) {
                harga_beli = parseInt(selectedOption.dataset.harga_beli);
            }

            keranjangPembelian.push({
                id_produk,
                nama,
                harga_beli,
                qty
            });
            updateKeranjangPembelianDisplay();
            document.getElementById('qty_produk_pembelian').value = '';
            document.getElementById('harga_beli_manual').value = '';
            select.value = '';
        });


        // ==================== MODAL TAMBAH PELANGGAN ====================
        const modalTambahPelanggan = document.getElementById('modalTambahPelanggan');
        document.getElementById('btnTambahPelanggan')?.addEventListener('click', () => {
            modalTambahPelanggan.classList.remove('hidden');
        });
        document.getElementById('closeTambahPelanggan')?.addEventListener('click', () => {
            modalTambahPelanggan.classList.add('hidden');
        });

        // Simpan keranjang ke LocalStorage sebelum submit form tambah pelanggan
        document.getElementById('formTambahPelanggan')?.addEventListener('submit', () => {
            localStorage.setItem('keranjang_penjualan', JSON.stringify(keranjang));
        });

        // ==================== MODAL TAMBAH SUPPLIER ====================
        const modalTambahSupplier = document.getElementById('modalTambahSupplier');
        document.getElementById('btnTambahSupplier')?.addEventListener('click', () => {
            modalTambahSupplier.classList.remove('hidden');
        });
        document.getElementById('closeTambahSupplier')?.addEventListener('click', () => {
            modalTambahSupplier.classList.add('hidden');
        });

        // Simpan keranjang ke LocalStorage sebelum submit form tambah supplier
        document.getElementById('formTambahSupplier')?.addEventListener('submit', () => {
            localStorage.setItem('keranjang_pembelian', JSON.stringify(keranjangPembelian));
        });

        // ==================== RESTORE KERANJANG DARI LOCALSTORAGE ====================
        const urlParams = new URLSearchParams(window.location.search);
        const bukaModal = urlParams.get('buka');

        if (bukaModal === 'penjualan') {
            const simpan = localStorage.getItem('keranjang_penjualan');
            if (simpan) {
                try { keranjang = JSON.parse(simpan); updateKeranjangDisplay(); } catch(e) {}
            }
            if (modalPenjualan) {
                modalPenjualan.classList.remove('hidden');
                document.body.classList.add('overflow-hidden');
            }
            localStorage.removeItem('keranjang_penjualan');
            history.replaceState(null, '', 'transaksi.php');
        }

        if (bukaModal === 'pembelian') {
            const simpan = localStorage.getItem('keranjang_pembelian');
            if (simpan) {
                try { keranjangPembelian = JSON.parse(simpan); updateKeranjangPembelianDisplay(); } catch(e) {}
            }
            if (modalPembelian) {
                modalPembelian.classList.remove('hidden');
                document.body.classList.add('overflow-hidden');
            }
            localStorage.removeItem('keranjang_pembelian');
            history.replaceState(null, '', 'transaksi.php');
        }

        window.onclick = function(event) {
            if (event.target === modalPenjualan) {
                modalPenjualan.classList.add('hidden');
                document.body.classList.remove('overflow-hidden');
            }
            if (event.target === modalPembelian) {
                modalPembelian.classList.add('hidden');
                document.body.classList.remove('overflow-hidden');
            }
        }

        // ==================== NOTIFIKASI ALERT & AUTO-HIDE ====================
        <?php if ($status == 'penjualan_sukses'): ?>
            alert('✅ Transaksi penjualan berhasil disimpan!');
            history.replaceState(null, '', 'transaksi.php');
        <?php elseif ($status == 'penjualan_gagal'): ?>
            alert('❌ Gagal menyimpan transaksi penjualan!');
        <?php elseif ($status == 'pembelian_sukses'): ?>
            alert('✅ Pembelian stok berhasil dicatat!');
            history.replaceState(null, '', 'transaksi.php');
        <?php elseif ($status == 'pembelian_gagal'): ?>
            alert('❌ Gagal mencatat pembelian stok!');
        <?php elseif ($status == 'pelanggan_sukses'): ?>
            alert('✅ Pelanggan baru berhasil ditambahkan!');
            history.replaceState(null, '', 'transaksi.php');
        <?php elseif ($status == 'pelanggan_gagal'): ?>
            alert('❌ Gagal menambahkan pelanggan baru!');
        <?php elseif ($status == 'supplier_sukses'): ?>
            alert('✅ Supplier baru berhasil ditambahkan!');
            history.replaceState(null, '', 'transaksi.php');
        <?php elseif ($status == 'supplier_gagal'): ?>
            alert('❌ Gagal menambahkan supplier baru!');
        <?php endif; ?>

        // Auto-hide notifikasi banner setelah 4 detik
        const notifBanner = document.getElementById('notif-banner');
        if (notifBanner) {
            setTimeout(() => {
                notifBanner.style.transition = 'opacity 0.5s ease';
                notifBanner.style.opacity = '0';
                setTimeout(() => notifBanner.remove(), 500);
            }, 4000);
        }
    </script>

</body>

</html>