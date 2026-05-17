<?php
include 'koneksi.php';

// Tabel Riwayat Transaksi
$query = "(SELECT tanggal_transaksi as tgl, 'Penjualan' as tipe, total_harga as nominal, metode_pembayaran as metode, status_bayar as status FROM penjualan) 
          UNION
          (SELECT tanggal as tgl, 'Pembelian' as tipe, total_biaya as nominal, 'TUNAI' as metode, 'Selesai' as status FROM pembelian) 
          ORDER BY tgl DESC";

$result = mysqli_query($conn, $query);


// Informasi Kartu (Lunas)
$query_kas = "SELECT SUM(total_harga) as total_kas FROM penjualan WHERE status_bayar='Lunas' AND metode_pembayaran='Tunai'";
$res_kas = mysqli_query($conn, $query_kas);
$data_kas = mysqli_fetch_assoc($res_kas);
$total_kas = $data_kas['total_kas'] ?? 0; // Jika NULL, set ke 0

// Informasi Kartu (Piutang)
$query_piutang = "SELECT SUM(sisa_tagihan) as total_piutang, COUNT(id_piutang) as jml_transaksi FROM piutang WHERE status='Belum Lunas'";
$res_piutang = mysqli_query($conn, $query_piutang);
$data_piutang = mysqli_fetch_assoc($res_piutang);
$total_piutang = $data_piutang['total_piutang'] ?? 0;
$jml_piutang = $data_piutang['jml_transaksi'] ?? 0;
?>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Transaksi</title>
     <link rel="stylesheet" href="dist/output.css">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;700&display=swap');
        body { font-family: 'Plus Jakarta Sans', sans-serif; }
    </style>
</head>
<body>
     <nav class="flex items-center justify-between px-8 py-4 bg-white shadow-sm sticky top-0 z-50">
        <div class="flex items-center gap-3">
            <button id="menuBtn" class="p-2 text-slate-600 hover:bg-slate-100 rounded-lg">
        <i class="fa-solid fa-bars text-lg"></i>
    </button>

    <div class="flex items-center gap-2">
        <img src="Assets/LogoBaru.png" alt="Kasly Logo" class="w-12 h-12 object-contain" >
    </div>
        </div>
        <div class="flex items-center gap-4">
            <button class="p-2 text-slate-500 hover:bg-slate-100 rounded-full transition">
                <i class="fa-regular fa-bell"></i>
            </button>
            <div class="w-10 h-10 rounded-full bg-indigo-100 border border-indigo-200 flex items-center justify-center text-indigo-600 font-bold">
                R
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
    <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
        <a href="utangPiutang.php" class="block p-3 w-full h-full">Utang & Piutang</a>
    </li>
    <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
        <a href="laporan.php" class="block p-3 w-full h-full">Laporan</a>
    </li>
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

<div class="max-w-7xl mx-auto px-8 sm:px-10 lg:px-14 pt-4 pb-8">  

    <section id="transaksi-page" class="space-y-6">
    
    <div class="flex flex-col md:flex-row md:items-center justify-between gap-4">
        <div>
            <h2 class="text-2xl font-bold text-slate-800">Catatan Kas & Transaksi</h2>
            <p class="text-slate-500 text-sm">Kelola semua arus kas masuk dan keluar di sini.</p>
        </div>
        <div class="flex flex-wrap gap-2">
            <button class="bg-rose-500 text-white px-4 py-2.5 rounded-xl text-sm font-bold hover:bg-rose-600 transition flex items-center gap-2 shadow-sm">
                <i class="fa-solid fa-file-invoice-dollar"></i> Catat Biaya
            </button>
            <button class="bg-indigo-600 text-white px-4 py-2.5 rounded-xl text-sm font-bold hover:bg-indigo-700 transition flex items-center gap-2 shadow-sm">
                <i class="fa-solid fa-cart-plus"></i> Input Penjualan
            </button>
        </div>
    </div>

    <div class="grid grid-cols-1 md:grid-cols-3 gap-8 lg:gap-12 xl:gap-16">
        <div class="bg-gradient-to-br from-indigo-600 to-violet-700 p-6 rounded-2xl text-white shadow-lg shadow-indigo-100 relative overflow-hidden">
            <div class="absolute top-0 right-0 -mr-8 -mt-8 w-32 h-32 bg-white opacity-10 rounded-full"></div>
            <div class="relative z-10">
                <p class="text-indigo-100 text-xs font-bold uppercase tracking-widest mb-1">Total Kas di Tangan</p>
                <h3 class="text-3xl font-black">Rp <?= number_format($total_kas, 0, ',', '.') ?></h3>
                <div class="mt-4 flex items-center gap-2 text-[10px] bg-white/20 w-fit px-2 py-1 rounded-md">
                    <i class="fa-solid fa-clock-rotate-left"></i> Real-time Database
                </div>
            </div>
        </div>

        <div class="bg-white border border-slate-200 p-6 rounded-2xl shadow-sm flex flex-col justify-between">
            <div class="flex justify-between items-start">
                <p class="text-slate-500 text-xs font-bold uppercase">Piutang Aktif</p>
                <span class="bg-amber-100 text-amber-600 text-[10px] px-2 py-1 rounded-full font-bold">
                    <?= $jml_piutang ?> Transaksi
                </span>
            </div>
            <h3 class="text-2xl font-black text-slate-800 mt-2">Rp <?= number_format($total_piutang, 0, ',', '.') ?></h3>
            <a href="utangPiutang.php" class="text-xs text-slate-400 mt-2 hover:text-indigo-600 cursor-pointer italic underline">
                Lihat semua piutang →
            </a>
        </div>

        <div class="bg-white border border-slate-200 p-6 rounded-2xl shadow-sm flex items-center gap-4">
            <div class="w-12 h-12 bg-slate-100 text-slate-400 rounded-2xl flex items-center justify-center">
                <i class="fa-solid fa-calendar-check text-xl"></i>
            </div>
            <div>
                <p class="text-slate-500 text-[10px] font-bold uppercase">Bayar Rutin</p>
                <p class="text-sm font-bold text-slate-800 italic">2 Tagihan besok</p>
                <button class="text-[10px] text-indigo-600 font-bold mt-1 uppercase tracking-tighter hover:tracking-normal transition-all">Kelola Jadwal</button>
            </div>
        </div>
    </div>

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
        <div class="flex items-center gap-2 px-3 py-2 bg-slate-10 rounded-xl border border-slate-100">
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

    <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
        <div class="overflow-x-auto">
            <table class="w-full text-left border-collapse">
                <thead class="bg-slate-50">
                    <tr>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Waktu & Tipe</th>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Keterangan</th>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Metode</th>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Status</th>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest text-right">Nominal</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-slate-100">
                    <?php while ($row = mysqli_fetch_assoc($result)) : 
                        // Logika Warna dan Ikon berdasarkan Tipe
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
        <div class="p-4 bg-slate-50 border-t border-slate-100 text-center">
            <button class="text-xs font-bold text-indigo-600 hover:underline">Muat Transaksi Lainnya...</button>
        </div>
    </div>
</section>
</div>
<script src="src/js/script.js"></script>

</body>
</html>