<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

$user_id = $_SESSION['user_id'];

$queryUser = "SELECT id_user, id_umkm, nama_lengkap, role FROM user WHERE id_user = '$user_id'";
$resultUser = mysqli_query($conn, $queryUser);

if (mysqli_num_rows($resultUser) === 0) {
    session_destroy();
    header("Location: login.php");
    exit();
}

$rowUser = mysqli_fetch_assoc($resultUser);
$inisial = strtoupper(substr($rowUser['nama_lengkap'], 0, 1));
$id_umkm = $rowUser['id_umkm'];

// ==================== AMBIL DATA DARI DATABASE ====================

// 1. Total Omzet (Penjualan Lunas)
$queryOmzet = "SELECT SUM(total_harga) as total FROM penjualan 
               WHERE id_umkm = '$id_umkm' AND status_bayar = 'Lunas'";
$resOmzet = mysqli_query($conn, $queryOmzet);
$dataOmzet = mysqli_fetch_assoc($resOmzet);
$totalOmzet = ($dataOmzet['total'] != '') ? $dataOmzet['total'] : 0;

// 2. Total Omzet Bulan Lalu untuk persentase
$lastMonth = date('Y-m', strtotime('-1 month'));
$queryOmzetLast = "SELECT SUM(total_harga) as total FROM penjualan 
                   WHERE id_umkm = '$id_umkm' AND status_bayar = 'Lunas' 
                   AND DATE_FORMAT(tanggal_transaksi, '%Y-%m') = '$lastMonth'";
$resOmzetLast = mysqli_query($conn, $queryOmzetLast);
$dataOmzetLast = mysqli_fetch_assoc($resOmzetLast);
$totalOmzetLast = ($dataOmzetLast['total'] != '') ? $dataOmzetLast['total'] : 0;

$omzetPersen = 0;
$omzetTrend = 'up';
if ($totalOmzetLast > 0) {
    $omzetPersen = round((($totalOmzet - $totalOmzetLast) / $totalOmzetLast) * 100, 1);
    $omzetTrend = $omzetPersen >= 0 ? 'up' : 'down';
} else if ($totalOmzet > 0) {
    $omzetPersen = 100;
    $omzetTrend = 'up';
}

// 3. Total Beban (Pembelian)
$queryBeban = "SELECT SUM(total_biaya) as total FROM pembelian WHERE id_umkm = '$id_umkm'";
$resBeban = mysqli_query($conn, $queryBeban);
$dataBeban = mysqli_fetch_assoc($resBeban);
$totalBeban = ($dataBeban['total'] != '') ? $dataBeban['total'] : 0;

// 4. Total Beban Bulan Lalu
$queryBebanLast = "SELECT SUM(total_biaya) as total FROM pembelian 
                   WHERE id_umkm = '$id_umkm' 
                   AND DATE_FORMAT(tanggal, '%Y-%m') = '$lastMonth'";
$resBebanLast = mysqli_query($conn, $queryBebanLast);
$dataBebanLast = mysqli_fetch_assoc($resBebanLast);
$totalBebanLast = ($dataBebanLast['total'] != '') ? $dataBebanLast['total'] : 0;

$bebanPersen = 0;
$bebanTrend = 'up';
if ($totalBebanLast > 0) {
    $bebanPersen = round((($totalBeban - $totalBebanLast) / $totalBebanLast) * 100, 1);
    $bebanTrend = $bebanPersen >= 0 ? 'up' : 'down';
} else if ($totalBeban > 0) {
    $bebanPersen = 100;
    $bebanTrend = 'up';
}

// 5. Laba Bersih
$labaBersih = $totalOmzet - $totalBeban;
$marginKeuntungan = $totalOmzet > 0 ? round(($labaBersih / $totalOmzet) * 100, 1) : 0;

// 6. Grafik Tren Penjualan (7 hari atau 30 hari dari parameter GET)
$periode = isset($_GET['periode']) ? $_GET['periode'] : '7';
$labels7 = [];
$values7 = [];
$hari = ($periode == '30') ? 30 : 7;

for ($i = $hari - 1; $i >= 0; $i--) {
    $tanggal = date('Y-m-d', strtotime("-$i days"));
    $labels7[] = date('d/m', strtotime($tanggal));
    
    $queryTren = "SELECT SUM(total_harga) as total FROM penjualan 
                  WHERE id_umkm = '$id_umkm' AND status_bayar = 'Lunas'
                  AND DATE(tanggal_transaksi) = '$tanggal'";
    $resTren = mysqli_query($conn, $queryTren);
    $dataTren = mysqli_fetch_assoc($resTren);
    $values7[] = ($dataTren['total'] != '') ? $dataTren['total'] : 0;
}
$maxTrenValue = !empty($values7) ? max($values7) : 1;

// 7. Alokasi Pengeluaran (dari stok masuk per kategori)
$queryAlokasi = "SELECT p.kategori, SUM(sm.jumlah_masuk * p.harga_beli) as total_biaya
                 FROM stok_masuk sm
                 JOIN produk p ON sm.id_produk = p.id_produk
                 WHERE p.id_umkm = '$id_umkm'
                 GROUP BY p.kategori";
$resAlokasi = mysqli_query($conn, $queryAlokasi);
$alokasiData = [];
$alokasiWarna = ['#6366f1', '#f43f5e', '#f59e0b', '#10b981', '#8b5cf6', '#06b6d4', '#ec4899'];
$warnaIndex = 0;
while ($row = mysqli_fetch_assoc($resAlokasi)) {
    $row['warna'] = $alokasiWarna[$warnaIndex % count($alokasiWarna)];
    $alokasiData[] = $row;
    $warnaIndex++;
}

// 8. 5 Produk Terlaris
$queryTop5 = "SELECT p.nama_produk, SUM(pd.kuantitas) as total_terjual
              FROM penjualan_detail pd
              JOIN produk p ON pd.id_produk = p.id_produk
              JOIN penjualan pj ON pd.id_penjualan = pj.id_penjualan
              WHERE pj.id_umkm = '$id_umkm' AND pj.status_bayar = 'Lunas'
              GROUP BY p.id_produk, p.nama_produk
              ORDER BY total_terjual DESC
              LIMIT 5";
$resTop5 = mysqli_query($conn, $queryTop5);
$top5Produk = [];
$maxTerjual = 0;
while ($row = mysqli_fetch_assoc($resTop5)) {
    $top5Produk[] = $row;
    if ($row['total_terjual'] > $maxTerjual) $maxTerjual = $row['total_terjual'];
}

// 9. Inventory Alert (Stok Menipis dengan estimasi)
$queryAlert = "SELECT p.nama_produk, p.sisa_stok,
               COALESCE(SUM(pd.kuantitas), 0) as total_terjual_30hari
               FROM produk p
               LEFT JOIN penjualan_detail pd ON p.id_produk = pd.id_produk
               LEFT JOIN penjualan pj ON pd.id_penjualan = pj.id_penjualan
               AND pj.tanggal_transaksi >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
               WHERE p.id_umkm = '$id_umkm' AND p.sisa_stok <= 15
               GROUP BY p.id_produk, p.nama_produk, p.sisa_stok
               ORDER BY p.sisa_stok ASC
               LIMIT 3";
$resAlert = mysqli_query($conn, $queryAlert);
$alertData = [];
while ($row = mysqli_fetch_assoc($resAlert)) {
    $rataHarian = $row['total_terjual_30hari'] / 30;
    $estimasiHari = ($rataHarian > 0) ? ceil($row['sisa_stok'] / $rataHarian) : 99;
    $row['estimasi_hari'] = $estimasiHari;
    $alertData[] = $row;
}
?>

<!DOCTYPE html>
<html lang="en">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Laporan - Kasly</title>
    <link rel="stylesheet" href="dist/output.css">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
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

    <section id="laporan-page" class="space-y-8 p-4 md:p-6">
        <!-- HEADER -->
        <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
            <div class="flex flex-col md:flex-row md:items-center justify-between gap-6">
                <div class="flex items-center gap-4">
                    <div class="w-14 h-14 bg-indigo-50 text-indigo-600 rounded-2xl flex items-center justify-center shadow-inner">
                        <i class="fa-solid fa-chart-pie text-2xl"></i>
                    </div>
                    <div>
                        <h2 class="text-2xl font-black text-slate-800 tracking-tight">Laporan Bisnis & Analitik</h2>
                        <p class="text-sm text-slate-500 font-medium">Pantau kesehatan keuangan dan performa penjualan secara real-time.</p>
                    </div>
                </div>
                <div class="flex flex-wrap gap-2">
                    <button onclick="window.print()" class="bg-white border border-slate-200 text-slate-600 px-5 py-2.5 rounded-xl text-xs font-bold hover:bg-slate-50 hover:scale-105 transition-all flex items-center gap-2 shadow-sm">
                        <i class="fa-solid fa-file-pdf text-rose-500"></i> Cetak Laporan
                    </button>
                </div>
            </div>
        </div>

        <!-- 3 CARD UTAMA (DATA DINAMIS) -->
        <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div class="bg-white p-6 rounded-3xl border border-slate-200 shadow-sm group hover:border-indigo-100 transition-all">
                <p class="text-slate-400 text-[10px] font-bold uppercase tracking-widest mb-2">Total Omzet</p>
                <h3 class="text-2xl font-black text-slate-800">Rp <?php echo number_format($totalOmzet, 0, ',', '.'); ?></h3>
                <div class="mt-3 flex items-center gap-1 <?php echo $omzetTrend == 'up' ? 'text-emerald-600' : 'text-rose-500'; ?> font-bold text-xs">
                    <i class="fa-solid fa-caret-<?php echo $omzetTrend; ?>"></i> <?php echo abs($omzetPersen); ?>%
                    <span class="text-slate-300 font-normal ml-1 italic">vs bulan lalu</span>
                </div>
            </div>

            <div class="bg-white p-6 rounded-3xl border border-slate-200 shadow-sm group hover:border-rose-100 transition-all">
                <p class="text-slate-400 text-[10px] font-bold uppercase tracking-widest mb-2">Total Beban/Biaya</p>
                <h3 class="text-2xl font-black text-slate-800">Rp <?php echo number_format($totalBeban, 0, ',', '.'); ?></h3>
                <div class="mt-3 flex items-center gap-1 <?php echo $bebanTrend == 'up' ? 'text-rose-500' : 'text-emerald-600'; ?> font-bold text-xs">
                    <i class="fa-solid fa-caret-<?php echo $bebanTrend; ?>"></i> <?php echo abs($bebanPersen); ?>%
                    <span class="text-slate-300 font-normal ml-1 italic">vs bulan lalu</span>
                </div>
            </div>

            <div class="bg-gradient-to-br from-indigo-600 to-violet-700 p-6 rounded-3xl shadow-xl shadow-indigo-100 text-white relative overflow-hidden">
                <div class="absolute top-0 right-0 -mr-4 -mt-4 w-24 h-24 bg-white/10 rounded-full blur-2xl"></div>
                <p class="text-indigo-100 text-[10px] font-bold uppercase tracking-widest mb-2">Laba Bersih</p>
                <h3 class="text-2xl font-black">Rp <?php echo number_format($labaBersih, 0, ',', '.'); ?></h3>
                <div class="mt-3 flex items-center gap-1 text-indigo-200 font-bold text-xs">
                    <i class="fa-solid fa-chart-line"></i> Margin Keuntungan <?php echo $marginKeuntungan; ?>%
                </div>
            </div>
        </div>

        <!-- TREN PENJUALAN + ALOKASI PENGELUARAN -->
        <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
            <div class="lg:col-span-2 bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
                <div class="flex justify-between items-center mb-6">
                    <h4 class="font-bold text-slate-800 flex items-center gap-2">
                        <i class="fa-solid fa-fire text-orange-500"></i> Tren Penjualan
                    </h4>
                    <select id="periodeSelect" class="bg-slate-50 border border-slate-100 text-[10px] font-bold p-2 rounded-xl outline-none cursor-pointer">
                        <option value="7" <?php echo $periode == '7' ? 'selected' : ''; ?>>7 Hari Terakhir</option>
                        <option value="30" <?php echo $periode == '30' ? 'selected' : ''; ?>>30 Hari Terakhir</option>
                    </select>
                </div>
                <canvas id="trenChart" height="200"></canvas>
            </div>

            <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
                <h4 class="font-bold text-slate-800 mb-6 uppercase text-[10px] tracking-widest">Alokasi Pengeluaran</h4>
                <?php if (count($alokasiData) > 0): ?>
                <div class="flex justify-center mb-6 relative">
                    <canvas id="alokasiChart" width="200" height="200"></canvas>
                </div>
                <div class="space-y-3">
                    <?php foreach ($alokasiData as $a): ?>
                    <div class="flex items-center justify-between p-2 hover:bg-slate-50 rounded-lg transition-colors">
                        <span class="flex items-center gap-2 text-[11px] font-medium text-slate-600">
                            <i class="fa-solid fa-circle" style="color: <?php echo $a['warna']; ?>; font-size: 6px;"></i> 
                            <?php echo htmlspecialchars($a['kategori']); ?>
                        </span>
                        <span class="text-xs font-black text-slate-700">Rp <?php echo number_format($a['total_biaya'], 0, ',', '.'); ?></span>
                    </div>
                    <?php endforeach; ?>
                </div>
                <?php else: ?>
                <div class="text-center py-8 text-slate-400">Belum ada data pengeluaran</div>
                <?php endif; ?>
            </div>
        </div>

        <!-- 5 PRODUK TERLARIS + INVENTORY ALERT -->
        <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
                <h4 class="font-bold text-slate-800 mb-6 flex items-center gap-2">
                    <i class="fa-solid fa-trophy text-amber-400 text-sm"></i> 5 Produk Terlaris
                </h4>
                <div class="space-y-5">
                    <?php if (count($top5Produk) > 0): ?>
                        <?php $rank = 1; foreach ($top5Produk as $produk):
                            $persen = $maxTerjual > 0 ? ($produk['total_terjual'] / $maxTerjual) * 100 : 0;
                        ?>
                        <div class="flex items-center gap-4 group">
                            <span class="w-6 text-xs font-black text-indigo-200 group-hover:text-indigo-500 transition-colors"><?php echo sprintf("%02d", $rank); ?></span>
                            <div class="flex-1">
                                <div class="flex justify-between mb-1.5">
                                    <p class="text-xs font-bold text-slate-700"><?php echo htmlspecialchars($produk['nama_produk']); ?></p>
                                    <p class="text-xs font-black text-slate-500"><?php echo $produk['total_terjual']; ?> pcs</p>
                                </div>
                                <div class="w-full bg-slate-100 h-2 rounded-full overflow-hidden">
                                    <div class="bg-indigo-600 h-full rounded-full" style="width: <?php echo $persen; ?>%"></div>
                                </div>
                            </div>
                        </div>
                        <?php $rank++; endforeach; ?>
                    <?php else: ?>
                        <div class="text-center py-8 text-slate-400">Belum ada data penjualan</div>
                    <?php endif; ?>
                </div>
            </div>

            <div class="bg-rose-50 border border-rose-100 rounded-3xl p-6 relative overflow-hidden">
                <div class="absolute -right-4 -bottom-4 text-rose-100/50 text-8xl transform -rotate-12">
                    <i class="fa-solid fa-box-archive"></i>
                </div>
                <h4 class="font-black text-rose-800 mb-6 flex items-center gap-2 uppercase text-[10px] tracking-widest">
                    <i class="fa-solid fa-triangle-exclamation"></i> Inventory Alert
                </h4>
                <div class="space-y-4 relative z-10">
                    <?php if (count($alertData) > 0): ?>
                        <?php foreach ($alertData as $alert):
                            $statusText = ($alert['estimasi_hari'] <= 3) ? 'Habis dalam ±' . $alert['estimasi_hari'] . ' hari' : 'Stok: ' . $alert['sisa_stok'] . ' pcs tersisa';
                            $statusClass = ($alert['estimasi_hari'] <= 3) ? 'text-rose-500' : 'text-amber-500';
                        ?>
                        <div class="bg-white p-4 rounded-2xl flex items-center justify-between border border-rose-200 shadow-sm hover:shadow-md transition-shadow">
                            <div class="flex items-center gap-3">
                                <div class="w-12 h-12 bg-rose-100 text-rose-600 rounded-xl flex items-center justify-center shadow-inner">
                                    <i class="fa-solid fa-box-open text-lg"></i>
                                </div>
                                <div>
                                    <p class="text-sm font-black text-slate-800"><?php echo htmlspecialchars($alert['nama_produk']); ?></p>
                                    <p class="text-[10px] <?php echo $statusClass; ?> font-bold uppercase tracking-tight">
                                        <?php echo $statusText; ?>
                                    </p>
                                </div>
                            </div>
                            <a href="produk.php" class="text-[10px] font-black bg-rose-600 text-white px-4 py-2 rounded-xl hover:bg-rose-700 shadow-lg shadow-rose-100 transition-all active:scale-95">RESTOK</a>
                        </div>
                        <?php endforeach; ?>
                    <?php else: ?>
                        <div class="bg-white p-4 rounded-2xl text-center">
                            <p class="text-sm font-medium text-slate-500">✨ Semua stok aman! ✨</p>
                            <p class="text-[10px] text-slate-400 mt-1">Tidak ada produk yang perlu segera di-restok.</p>
                        </div>
                    <?php endif; ?>
                </div>
            </div>
        </div>
    </section>

    <script src="src/js/script.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script>
        // Grafik Tren Penjualan
        const labels = <?php echo json_encode($labels7); ?>;
        const values = <?php echo json_encode($values7); ?>;
        
        const ctx = document.getElementById('trenChart').getContext('2d');
        let trenChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Omzet (Rp)',
                    data: values,
                    borderColor: '#6366f1',
                    backgroundColor: 'rgba(99, 102, 241, 0.1)',
                    tension: 0.3,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { position: 'top' },
                    tooltip: { callbacks: { label: (ctx) => `Rp ${ctx.raw.toLocaleString('id-ID')}` } }
                },
                scales: {
                    y: { 
                        ticks: { callback: (val) => `Rp ${val.toLocaleString('id-ID')}` },
                        beginAtZero: true
                    }
                }
            }
        });

        // Grafik Alokasi Pengeluaran (Donut)
        <?php if (count($alokasiData) > 0): ?>
        const alokasiLabels = <?php 
            $alabels = [];
            $avalues = [];
            foreach ($alokasiData as $a) {
                $alabels[] = $a['kategori'];
                $avalues[] = $a['total_biaya'];
            }
            echo json_encode($alabels);
        ?>;
        const alokasiValues = <?php echo json_encode($avalues); ?>;
        const alokasiWarna = <?php 
            $awarna = [];
            foreach ($alokasiData as $a) {
                $awarna[] = $a['warna'];
            }
            echo json_encode($awarna);
        ?>;
        
        new Chart(document.getElementById('alokasiChart'), {
            type: 'doughnut',
            data: {
                labels: alokasiLabels,
                datasets: [{
                    data: alokasiValues,
                    backgroundColor: alokasiWarna,
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: true,
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: (ctx) => `${ctx.label}: Rp ${ctx.raw.toLocaleString('id-ID')}` } }
                }
            }
        });
        <?php endif; ?>

        // Filter periode (reload dengan parameter)
        document.getElementById('periodeSelect').addEventListener('change', function() {
            window.location.href = 'laporan.php?periode=' + this.value;
        });
    </script>
</body>

</html>