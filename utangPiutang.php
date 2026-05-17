<?php
session_start();

// 1. Validasi Sesi Pengguna
if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit(); 
}

require_once 'koneksi.php'; 

$user_id = $_SESSION['user_id'];

// 2. Mengambil Data User untuk mendapatkan id_umkm
$query_user = "SELECT id_user, id_umkm, nama_lengkap FROM user WHERE id_user = '$user_id'";
$result_user = mysqli_query($conn, $query_user);

if (mysqli_num_rows($result_user) === 0) {
    session_destroy();
    header("Location: login.php");
    exit();
}

$row = mysqli_fetch_assoc($result_user);
$inisial = strtoupper(substr($row['nama_lengkap'], 0, 1));
$id_umkm = $row['id_umkm'];


// 3. Mengambil Data Ringkasan Dashboard (Sangat Efektif & Singkat)
$query_ringkasan = "SELECT 
                        -- Hitung piutang UMKM ini langsung dari tabel penjualan
                        (SELECT SUM(p.sisa_tagihan) 
                         FROM piutang p
                         INNER JOIN penjualan pj ON p.id_penjualan = pj.id_penjualan
                         WHERE pj.id_umkm = '$id_umkm') AS sisa_piutang_all,
                        
                        -- Hitung utang UMKM ini langsung dari tabel pembelian
                        (SELECT SUM(ut.sisa_utang) 
                         FROM utang ut
                         INNER JOIN pembelian pb ON ut.id_pembelian = pb.id_pembelian
                         WHERE pb.id_umkm = '$id_umkm') AS sisa_utang_all";

$result_ringkasan = mysqli_query($conn, $query_ringkasan);

if (!$result_ringkasan) {
    die("Query Ringkasan Error: " . mysqli_error($conn));
}

$ringkasan = mysqli_fetch_assoc($result_ringkasan);
$sisa_piutang_dashboard = $ringkasan['sisa_piutang_all'] ?? 0;
$sisa_utang_dashboard   = $ringkasan['sisa_utang_all'] ?? 0; 
$saldo_bersih           = $sisa_piutang_dashboard - $sisa_utang_dashboard;


// 4. Mengambil Daftar Piutang Belum Lunas (Saring berdasarkan penjualan.id_umkm)
$query_tabel = "SELECT 
                    p.id_piutang,
                    pl.nama_pelanggan,
                    p.jatuh_tempo,
                    p.sisa_tagihan,
                    p.status
                FROM piutang p
                INNER JOIN pelanggan pl ON p.id_pelanggan = pl.id_pelanggan
                INNER JOIN penjualan pj ON p.id_penjualan = pj.id_penjualan
                WHERE pj.id_umkm = '$id_umkm' AND p.status != 'Lunas'
                ORDER BY p.jatuh_tempo ASC";

$result_tabel = mysqli_query($conn, $query_tabel);

$query_utang = "SELECT 
                    ut.id_utang,
                    s.nama_supplier,
                    ut.jatuh_tempo,
                    ut.sisa_utang,
                    ut.status
                FROM utang ut
                INNER JOIN supplier s ON ut.id_supplier = s.id_supplier
                INNER JOIN pembelian pb ON ut.id_pembelian = pb.id_pembelian
                WHERE pb.id_umkm = '$id_umkm' AND ut.status != 'Lunas'
                ORDER BY ut.jatuh_tempo ASC";

$result_utang = mysqli_query($conn, $query_utang);

if (!$result_tabel || !$result_utang) {
    die("Query Data Tabel Error: " . mysqli_error($conn));
}

$query_aktivitas = "
    (SELECT 
        pj.tanggal_transaksi AS tanggal,
        pl.nama_pelanggan AS nama,
        'Piutang' AS jenis,
        p.status AS status,
        pj.total_harga AS nominal
     FROM piutang p
     INNER JOIN pelanggan pl ON p.id_pelanggan = pl.id_pelanggan
     INNER JOIN penjualan pj ON p.id_penjualan = pj.id_penjualan
     WHERE pj.id_umkm = '$id_umkm')
    
    UNION ALL
    
    (SELECT 
        pb.tanggal AS tanggal,
        s.nama_supplier AS nama,
        'Utang' AS jenis,
        ut.status AS status,
        pb.total_biaya AS nominal
     FROM utang ut
     INNER JOIN supplier s ON ut.id_supplier = s.id_supplier
     INNER JOIN pembelian pb ON ut.id_pembelian = pb.id_pembelian
     WHERE pb.id_umkm = '$id_umkm')
    
    ORDER BY tanggal DESC 
    LIMIT 5"; // Kita batasi hanya menampilkan 5 aktivitas teratas

$result_aktivitas = mysqli_query($conn, $query_aktivitas);

if (!$result_aktivitas) {
    die("Query Aktivitas Error: " . mysqli_error($conn));
}

?>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Utang & Piutang</title>
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

    <!-- MANAJEMEN UTANG & PIUTANG -->

  <section id="utang-piutang-page" class="space-y-8 p-4 md:p-6"> <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm mb-8">
        <div class="flex flex-col md:flex-row md:items-center justify-between gap-6">
            <div class="flex items-center gap-4">
                <div class="w-14 h-14 bg-indigo-100 text-indigo-600 rounded-2xl flex items-center justify-center shadow-inner">
                    <i class="fa-solid fa-scale-balanced text-2xl"></i>
                </div>
                <div>
                    <h2 class="text-2xl font-black text-slate-800 tracking-tight">Manajemen Utang & Piutang</h2>
                    <p class="text-sm text-slate-500 font-medium">Pantau tagihan pelanggan dan kewajiban ke supplier.</p>
                </div>
            </div>

            <div class="flex flex-wrap gap-3">
                <button class="bg-indigo-600 text-white px-5 py-2.5 rounded-xl text-sm font-bold hover:bg-indigo-700 hover:scale-105 transition-all flex items-center gap-2 shadow-lg shadow-indigo-100">
                    <i class="fa-solid fa-plus text-[10px]"></i> Piutang
                </button>
                <button class="bg-slate-800 text-white px-5 py-2.5 rounded-xl text-sm font-bold hover:bg-slate-900 hover:scale-105 transition-all flex items-center gap-2 shadow-lg shadow-slate-200">
                    <i class="fa-solid fa-plus text-[10px]"></i> Utang
                </button>
                <div class="h-10 w-px bg-slate-200 mx-1 hidden md:block"></div> 
            </div>
        </div>
    </div>

    <!-- PIUTANG, UTANG, & SALDO BERSIH -->

    <div class="grid grid-cols-1 md:grid-cols-3 gap-6">
        <div class="bg-white p-6 rounded-2xl border border-slate-200 shadow-sm group hover:border-rose-200 transition-colors">
            <p class="text-slate-500 text-xs font-bold uppercase tracking-wider mb-1">Piutang Belum Lunas</p>
            <p class="text-2xl font-black text-rose-600">
                Rp <?php echo number_format($sisa_piutang_dashboard, 0, ',', '.'); ?>
            </p>
        </div>

        <div class="bg-white p-6 rounded-2xl border border-slate-200 shadow-sm group hover:border-indigo-200 transition-colors">
            <p class="text-slate-500 text-xs font-bold uppercase tracking-wider mb-1">Utang Belum Dibayar</p>
            <p class="text-2xl font-black text-indigo-600">
                Rp <?php echo number_format($sisa_utang_dashboard, 0, ',', '.'); ?>
            </p>
        </div>

        <div class="bg-indigo-50 p-6 rounded-2xl border border-indigo-100 shadow-sm">
            <p class="text-indigo-600 text-xs font-bold uppercase tracking-wider mb-1">Saldo Bersih (Piutang - Utang)</p>
            <p class="text-2xl font-black text-indigo-800">
                <?php if ($saldo_bersih < 0) : ?>
                    - Rp <?php echo number_format(abs($saldo_bersih), 0, ',', '.'); ?>
                <?php else : ?>
                    Rp <?php echo number_format($saldo_bersih, 0, ',', '.'); ?>
                <?php endif; ?>
            </p>
        </div>
    </div>

    <!-- PIUTANG PELANGGAN  -->
    <div class="grid grid-cols-1 lg:grid-cols-2 gap-8">
            
            <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
                <div class="p-6 border-b border-slate-100 flex justify-between items-center bg-slate-50/50">
                    <h3 class="font-bold text-slate-800 flex items-center gap-2 uppercase text-xs tracking-widest">
                        <i class="fa-solid fa-hand-holding-dollar text-emerald-500"></i> Piutang Pelanggan
                    </h3>
                </div>
                <div class="p-4 space-y-4">
                    <?php 
                    if (mysqli_num_rows($result_tabel) > 0) :
                        mysqli_data_seek($result_tabel, 0); 
                        while ($piutang = mysqli_fetch_assoc($result_tabel)) : 
                            if ($piutang['status'] !== 'Lunas') :
                                $tanggal_indo = date('d F Y', strtotime($piutang['jatuh_tempo']));
                                $bulan_eng = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
                                $bulan_indo = ['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'];
                                $tanggal_indo = str_replace($bulan_eng, $bulan_indo, $tanggal_indo);
                    ?>
                                <div class="p-4 border border-slate-100 rounded-2xl hover:bg-slate-50 transition-all border-l-4 border-l-rose-500">
                                    <div class="flex justify-between items-start mb-3">
                                        <div>
                                            <h4 class="font-bold text-slate-800"><?php echo htmlspecialchars($piutang['nama_pelanggan']); ?></h4>
                                            <p class="text-xs text-slate-500">Jatuh tempo: <span class="text-rose-500 font-medium"><?php echo $tanggal_indo; ?></span></p>
                                        </div>
                                        <span class="bg-rose-100 text-rose-600 text-[10px] px-2 py-1 rounded-full font-bold uppercase">Jatuh Tempo</span>
                                    </div>
                                    <div class="flex justify-between items-end">
                                        <p class="text-xl font-black text-slate-800">Rp <?php echo number_format($piutang['sisa_tagihan'], 0, ',', '.'); ?></p>
                                        <div class="flex gap-2">
                                            <a href="proses_lunas.php?id=<?php echo $piutang['id_piutang']; ?>" 
                                            onclick="return confirm('Apakah Anda yakin ingin menandai piutang ini sebagai Lunas?')" 
                                            class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg text-xs font-bold hover:bg-indigo-700 transition inline-block text-center">
                                            Tandai Lunas
                                            </a>
                                        </div>
                                    </div>
                                </div>
                    <?php 
                            endif;
                        endwhile; 
                    else :
                    ?>
                        <div class="p-6 text-center text-sm text-slate-400 border border-dashed border-slate-200 rounded-2xl">Tidak ada tagihan piutang aktif saat ini.</div>
                    <?php endif; ?>
                </div>
            </div>

            <!-- UTANG KE SUPPLIER -->

            <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
                <div class="p-6 border-b border-slate-100 bg-slate-50/50">
                    <h3 class="font-bold text-slate-800 flex items-center gap-2 uppercase text-xs tracking-widest">
                        <i class="fa-solid fa-truck-field text-indigo-500"></i> Utang ke Supplier
                    </h3>
                </div>
                <div class="p-4 space-y-4">
                    <?php 
                    if (mysqli_num_rows($result_utang) > 0) :
                        while ($utang = mysqli_fetch_assoc($result_utang)) : 
                            $tanggal_indo_utang = date('d F Y', strtotime($utang['jatuh_tempo']));
                            $bulan_eng = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
                            $bulan_indo = ['Januari', 'Februari', 'Maret', 'Mei', 'Juni', 'Juli', 'Agustus', 'September', 'Oktober', 'November', 'Desember'];
                            $tanggal_indo_utang = str_replace($bulan_eng, $bulan_indo, $tanggal_indo_utang);
                    ?>
                            <div class="p-4 border border-slate-100 rounded-2xl hover:bg-slate-50 transition-all border-l-4 border-l-indigo-500">
                                <div class="flex justify-between items-start mb-3">
                                    <div>
                                        <h4 class="font-bold text-slate-800"><?php echo htmlspecialchars($utang['nama_supplier']); ?></h4>
                                        <p class="text-xs text-slate-500">Jatuh tempo: <span class="text-indigo-500 font-medium"><?php echo $tanggal_indo_utang; ?></span></p>
                                    </div>
                                    <span class="bg-indigo-100 text-indigo-600 text-[10px] px-2 py-1 rounded-full font-bold uppercase">Tagihan</span>
                                </div>
                                <div class="flex justify-between items-end">
                                    <p class="text-xl font-black text-slate-800">Rp <?php echo number_format($utang['sisa_utang'], 0, ',', '.'); ?></p>
                                    <div class="flex gap-2">
                                        <a href="proses_bayar_utang.php?id=<?php echo $utang['id_utang']; ?>" 
                                        onclick="return confirm('Apakah Anda yakin ingin menandai utang ke supplier ini sebagai Lunas?')" 
                                        class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg text-xs font-bold hover:bg-indigo-700 transition inline-block text-center">
                                            Bayar Lunas
                                        </a>
                                    </div>
                                </div>
                            </div>
                    <?php 
                        endwhile; 
                    else :
                    ?>
                        <div class="text-center py-12">
                            <div class="w-16 h-16 bg-slate-50 rounded-full flex items-center justify-center mx-auto mb-3">
                                <i class="fa-solid fa-check-double text-slate-300 text-xl"></i>
                            </div>
                            <p class="text-sm font-bold text-slate-500">Semua utang lunas!</p>
                            <p class="text-xs text-slate-400">Tidak ada tagihan supplier yang perlu dibayar.</p>
                        </div>
                    <?php endif; ?>
                </div>
            </div>
        </div>

        <!-- AKTIVITAS TERBARU -->
        
        <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
            <div class="p-6 border-b border-slate-100 flex justify-between items-center bg-slate-50/50">
                <h3 class="font-bold text-slate-800 uppercase text-xs tracking-widest">Riwayat Aktivitas Terbaru</h3>
                <button class="text-indigo-600 text-[10px] font-black uppercase hover:underline">Lihat Semua</button>
            </div>
            <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse">
                    <thead class="bg-slate-50">
                        <tr>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Tanggal</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Nama</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Jenis</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider text-center">Status</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider text-right">Nominal</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100">
                        <?php 
                        if (mysqli_num_rows($result_aktivitas) > 0) :
                            while ($aktivitas = mysqli_fetch_assoc($result_aktivitas)) : 
                                
                                $tgl = date('d M Y', strtotime($aktivitas['tanggal']));
                                $bulan_eng = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                                $bulan_indo = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des'];
                                $tgl = str_replace($bulan_eng, $bulan_indo, $tgl);
                                
                                $is_lunas = ($aktivitas['status'] === 'Lunas');
                                $badge_status_class = $is_lunas 
                                    ? "bg-emerald-100 text-emerald-600" 
                                    : "bg-rose-100 text-rose-600";
                        ?>
                                <tr class="hover:bg-slate-50 transition">
                                    <td class="p-4 text-xs text-slate-500"><?php echo $tgl; ?></td>
                                    <td class="p-4 text-sm font-bold text-slate-700"><?php echo htmlspecialchars($aktivitas['nama']); ?></td>
                                    <td class="p-4 text-xs text-slate-500 font-medium"><?php echo $aktivitas['jenis']; ?></td>
                                    <td class="p-4 text-center">
                                        <span class="<?php echo $badge_status_class; ?> text-[10px] px-2 py-1 rounded-full font-black uppercase">
                                            <?php echo $aktivitas['status']; ?>
                                        </span>
                                    </td>
                                    
                                    <td class="p-4 text-sm font-black text-right text-slate-800">
                                        Rp <?php echo number_format($aktivitas['nominal'], 0, ',', '.'); ?>
                                    </td>
                                </tr>
                        <?php 
                            endwhile; 
                        else :
                        ?>
                            <tr>
                                <td colspan="5" class="p-8 text-center text-sm text-slate-400">
                                    Belum ada aktivitas transaksi utang atau piutang.
                                </td>
                            </tr>
                        <?php endif; ?>
                    </tbody>
                </table>
            </div>
        </div>
    </section>

    <script src="src/js/script.js"></script>
</body>
</html>