<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

$user_id = $_SESSION['user_id'];

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


$query_ringkasan = "SELECT 
                        (SELECT SUM(p.sisa_tagihan) 
                         FROM piutang p
                         INNER JOIN penjualan pj ON p.id_penjualan = pj.id_penjualan
                         WHERE pj.id_umkm = '$id_umkm') AS sisa_piutang_all,
                        
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
    LIMIT 5";

$result_aktivitas = mysqli_query($conn, $query_aktivitas);

if (!$result_aktivitas) {
    die("Query Aktivitas Error: " . mysqli_error($conn));
}

$query_dropdown_pembelian = "SELECT DISTINCT 
                                pb.id_pembelian, 
                                ut.id_supplier, 
                                pb.tanggal, 
                                ut.sisa_utang,
                                ut.jatuh_tempo
                             FROM pembelian pb
                             INNER JOIN utang ut ON pb.id_pembelian = ut.id_pembelian
                             INNER JOIN supplier s ON ut.id_supplier = s.id_supplier
                             WHERE pb.id_umkm = '$id_umkm' 
                             AND pb.status_bayar = 'Belum Lunas'
                             ORDER BY pb.id_pembelian DESC";
$result_dropdown_pembelian = mysqli_query($conn, $query_dropdown_pembelian);

$query_dropdown_supplier = "SELECT id_supplier, nama_supplier FROM supplier ORDER BY nama_supplier ASC";
$result_dropdown_supplier = mysqli_query($conn, $query_dropdown_supplier);

$query_dropdown_penjualan = "SELECT DISTINCT 
                                pj.id_penjualan, 
                                p.id_pelanggan, 
                                pj.tanggal_transaksi, 
                                p.sisa_tagihan 
                             FROM penjualan pj
                             INNER JOIN piutang p ON pj.id_penjualan = p.id_penjualan
                             WHERE pj.id_umkm = '$id_umkm' 
                             AND pj.status_bayar = 'Belum Lunas'
                             ORDER BY pj.id_penjualan DESC";

$result_dropdown_penjualan = mysqli_query($conn, $query_dropdown_penjualan);
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

        body {
            font-family: 'Plus Jakarta Sans', sans-serif;
        }
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

    <section id="utang-piutang-page" class="space-y-8 p-4 md:p-6">
        <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm mb-8">
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

                <div class="flex flex-wrap gap-2">
                    <button id="btnPiutang" class="bg-white border border-slate-200 text-slate-600 px-5 py-2.5 rounded-xl text-xs font-bold hover:bg-slate-50 hover:scale-105 transition-all flex items-center gap-2 shadow-sm">
                        <i class="fa-solid fa-plus text-rose-500"></i> Piutang
                    </button>
                    <button id="btnUtang" class="bg-white border border-slate-200 text-slate-600 px-5 py-2.5 rounded-xl text-xs font-bold hover:bg-slate-50 hover:scale-105 transition-all flex items-center gap-2 shadow-sm">
                        <i class="fa-solid fa-plus text-emerald-500"></i> Utang
                    </button>
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


        <!-- POP UP PIUTANG -->

        <div id="modalPiutang" class="hidden fixed inset-0 bg-black/50 z-50 flex items-start justify-center py-10 px-4 overflow-y-auto">
            <div class="bg-white w-full max-w-3xl rounded-3xl p-6 relative overflow-y-auto max-h-[90vh] overflow-y-auto">

                <button id="closePiutang" class="absolute top-5 right-5 w-10 h-10 rounded-full bg-slate-100 hover:bg-red-100 text-slate-500 hover:text-red-500 transition flex items-center justify-center">
                    <i class="fa-solid fa-xmark text-lg"></i>
                </button>

                <!-- PENCATATAN PIUTANG -->
                <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
                    <div class="flex items-center gap-3 mb-6 border-b border-slate-100 pb-4">
                        <div class="w-10 h-10 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center">
                            <i class="fa-solid fa-hand-holding-dollar text-lg"></i>
                        </div>
                        <div>
                            <h3 class="text-base font-black text-slate-800 tracking-tight">Pencatatan Piutang Baru</h3>
                            <p class="text-xs text-slate-400 font-medium">Catat dan kelola tagihan yang harus dibayarkan oleh pelanggan dari transaksi penjualan.</p>
                        </div>
                    </div>
                    <form action="proses_piutang.php" method="POST" class="space-y-4">
                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Nota / ID Penjualan</label>
                                <select name="id_penjualan" id="id_penjualan" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-700 appearance-none">
                                    <option value="">-- Pilih Nota Jual --</option>
                                    <?php
                                    if ($result_dropdown_penjualan && mysqli_num_rows($result_dropdown_penjualan) > 0) {
                                        while ($penjualan = mysqli_fetch_assoc($result_dropdown_penjualan)) {
                                    ?>
                                            <option value="<?php echo $penjualan['id_penjualan']; ?>"
                                                data-pelanggan="<?php echo $penjualan['id_pelanggan']; ?>"
                                                data-tagihan="<?php echo $penjualan['sisa_tagihan']; ?>">
                                                Nota #<?php echo $penjualan['id_penjualan']; ?> (Sisa: Rp <?php echo number_format($penjualan['sisa_tagihan'], 0, ',', '.'); ?>)
                                            </option>
                                    <?php
                                        }
                                    }
                                    ?>
                                </select>
                            </div>
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Nama Pelanggan</label>
                                <select name="id_pelanggan" id="id_pelanggan" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-700 appearance-none">
                                    <option value="">-- Pilih Pelanggan --</option>
                                    <?php
                                    $query_pelanggan = "SELECT id_pelanggan, nama_pelanggan FROM pelanggan ORDER BY nama_pelanggan ASC";
                                    $result_pelanggan = mysqli_query($conn, $query_pelanggan);
                                    while ($pelanggan = mysqli_fetch_assoc($result_pelanggan)):
                                    ?>
                                        <option value="<?php echo $pelanggan['id_pelanggan']; ?>"><?php echo htmlspecialchars($pelanggan['nama_pelanggan']); ?></option>
                                    <?php endwhile; ?>
                                </select>
                            </div>
                        </div>

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Total Sisa Tagihan (Rp)</label>
                                <input type="number" name="sisa_tagihan" id="sisa_tagihan" placeholder="0" readonly required
                                    class="w-full p-3 bg-slate-100 border border-slate-200 rounded-xl outline-none text-sm font-medium text-slate-500 cursor-not-allowed">
                            </div>
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Nominal Yang Dibayar (Rp)</label>
                                <input type="number" name="nominal_bayar" id="nominal_bayar" placeholder="Masukkan jumlah bayar" required
                                    class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-800">
                            </div>
                        </div>

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Tanggal Jatuh Tempo</label>
                                <input type="date" name="jatuh_tempo" required
                                    class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-600">
                            </div>
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Status Tagihan</label>
                                <select name="status" id="status_tagihan" required
                                    class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-700">
                                    <option value="Belum Lunas">Belum Lunas</option>
                                    <option value="Lunas">Lunas</option>
                                </select>
                            </div>
                        </div>

                        <div class="flex justify-end pt-2">
                            <button type="submit" name="btn_simpan_piutang" class="w-full px-6 py-2.5 bg-indigo-600 text-white rounded-xl text-xs font-bold hover:bg-indigo-700 hover:scale-[1.02] transition-all shadow-md shadow-indigo-100 flex items-center justify-center gap-2">
                                <i class="fa-solid fa-floppy-disk"></i> Simpan Data Piutang
                            </button>
                        </div>
                    </form>
                </div>

            </div>
        </div>

        <!-- POP UP UTANG -->

        <div id="modalUtang" class="hidden fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
            <div class="bg-white w-full max-w-3xl rounded-3xl p-6 relative overflow-y-auto max-h-[90vh]">

                <div class="relative mb-4">

                    <button id="closeUtang" class="absolute top-0 right-0 w-10 h-10 rounded-full bg-slate-100 hover:bg-red-100 text-slate-500 hover:text-red-500 transition flex items-center justify-center">
                        <i class="fa-solid fa-xmark text-lg"></i>
                    </button>

                </div>

                <!-- PENCATATAN UTANG -->

                <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
                    <div class="flex items-center gap-3 mb-6 border-b border-slate-100 pb-4">
                        <div class="w-10 h-10 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center">
                            <i class="fa-solid fa-hand-holding text-lg"></i>
                        </div>
                        <div>
                            <h3 class="text-base font-black text-slate-800 tracking-tight">Pencatatan Utang</h3>
                            <p class="text-xs text-slate-400 font-medium">Kurangi kewajiban pembayaran belanja ke supplier melalui cicilan.</p>
                        </div>
                    </div>

                    <form action="proses_utang.php" method="POST" class="space-y-4">
                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Nota / ID Pembelian</label>
                                <select name="id_pembelian" id="id_pembelian" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-700 appearance-none">
                                    <option value="">-- Pilih Nota Belanja --</option>
                                    <?php
                                    if ($result_dropdown_pembelian && mysqli_num_rows($result_dropdown_pembelian) > 0) {
                                        while ($pembelian = mysqli_fetch_assoc($result_dropdown_pembelian)) {
                                    ?>
                                            <option value="<?php echo $pembelian['id_pembelian']; ?>"
                                                data-supplier="<?php echo $pembelian['id_supplier']; ?>"
                                                data-utang="<?php echo $pembelian['sisa_utang']; ?>"
                                                data-tempo="<?php echo $pembelian['jatuh_tempo']; ?>">
                                                Nota #<?php echo $pembelian['id_pembelian']; ?> (Sisa: Rp <?php echo number_format($pembelian['sisa_utang'], 0, ',', '.'); ?>)
                                            </option>
                                    <?php
                                        }
                                    }
                                    ?>
                                </select>
                            </div>

                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Nama Supplier</label>
                                <select name="id_supplier" id="id_supplier" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-700 appearance-none">
                                    <option value="">-- Pilih Supplier --</option>
                                    <?php
                                    if ($result_dropdown_supplier && mysqli_num_rows($result_dropdown_supplier) > 0) {
                                        mysqli_data_seek($result_dropdown_supplier, 0);
                                        while ($supplier = mysqli_fetch_assoc($result_dropdown_supplier)) {
                                    ?>
                                            <option value="<?php echo trim($supplier['id_supplier']); ?>">
                                                <?php echo htmlspecialchars($supplier['nama_supplier']); ?>
                                            </option>
                                    <?php
                                        }
                                    }
                                    ?>
                                </select>
                            </div>
                        </div>

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Total Sisa Utang Lama (Rp)</label>
                                <input type="number" name="sisa_utang" id="sisa_utang" placeholder="0" readonly required
                                    class="w-full p-3 bg-slate-100 border border-slate-200 rounded-xl outline-none text-sm font-medium text-slate-500 cursor-not-allowed">
                            </div>
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Nominal Yang Dibayar (Rp)</label>
                                <input type="number" name="nominal_bayar" id="nominal_bayar_utang" placeholder="Masukkan jumlah bayar" required
                                    class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-800">
                            </div>
                        </div>

                        <div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Tanggal Jatuh Tempo</label>
                                <input type="date" name="jatuh_tempo" id="id_jatuh_tempo_utang" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-600">
                            </div>
                            <div class="space-y-2">
                                <label class="text-xs font-black text-slate-500 uppercase">Status Pembayaran</label>
                                <select name="status" id="status_utang" required class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-700">
                                    <option value="Belum Lunas">Belum Lunas</option>
                                    <option value="Lunas">Lunas</option>
                                </select>
                            </div>
                        </div>

                        <div class="flex justify-end pt-2">
                            <button type="submit" name="btn_simpan_utang" class="w-full px-6 py-2.5 bg-slate-800 text-white rounded-xl text-xs font-bold hover:bg-slate-900 hover:scale-[1.02] transition-all shadow-md flex items-center justify-center gap-2">
                                <i class="fa-solid fa-floppy-disk"></i> Simpan Data Utang
                            </button>
                        </div>
                    </form>
                </div>

            </div>
        </div>


        </div>
    </section>

    <script src="src/js/script.js"></script>
    <script>
        document.addEventListener("DOMContentLoaded", function() {

            const selectPembelian = document.getElementById('id_pembelian');
            const selectSupplier = document.getElementById('id_supplier');
            const inputSisaUtang = document.getElementById('sisa_utang');
            const inputNominalBayarUtang = document.getElementById('nominal_bayar_utang');
            const statusUtang = document.getElementById('status_utang');
            const inputJatuhTempoUtang = document.getElementById('id_jatuh_tempo_utang'); // Target input tanggal

            if (selectPembelian && selectSupplier) {
                selectPembelian.addEventListener('change', function() {
                    const selectedOption = this.options[this.selectedIndex];
                    const idSupplierTerkait = selectedOption.getAttribute('data-supplier');
                    const sisaUtangAktif = selectedOption.getAttribute('data-utang');
                    const jatuhTempoAktif = selectedOption.getAttribute('data-tempo'); // Ambil tanggal dari atribut data-tempo

                    if (idSupplierTerkait) {
                        selectSupplier.value = idSupplierTerkait;
                        inputSisaUtang.value = sisaUtangAktif;
                        inputJatuhTempoUtang.value = jatuhTempoAktif; // Otomatis isi tanggal jatuh tempo lama ke input date
                        inputNominalBayarUtang.max = sisaUtangAktif;
                        inputNominalBayarUtang.value = "";

                        selectSupplier.classList.add('bg-slate-100', 'cursor-not-allowed');
                        selectSupplier.style.pointerEvents = "none";
                    } else {
                        selectSupplier.value = "";
                        inputSisaUtang.value = "";
                        inputJatuhTempoUtang.value = ""; // Kosongkan jika default dipilih
                        inputNominalBayarUtang.value = "";
                        selectSupplier.classList.remove('bg-slate-100', 'cursor-not-allowed');
                        selectSupplier.style.pointerEvents = "auto";
                    }
                });

                if (inputNominalBayarUtang) {
                    inputNominalBayarUtang.addEventListener('input', function() {
                        const sisa = parseFloat(inputSisaUtang.value) || 0;
                        const bayar = parseFloat(this.value) || 0;

                        if (bayar >= sisa && sisa > 0) {
                            statusUtang.value = "Lunas";
                        } else {
                            statusUtang.value = "Belum Lunas";
                        }
                    });
                }
            }

            const selectPenjualan = document.getElementById('id_penjualan');
            const selectPelanggan = document.getElementById('id_pelanggan');
            const inputSisaTagihan = document.getElementById('sisa_tagihan');
            const inputNominalBayar = document.getElementById('nominal_bayar');
            const statusTagihan = document.getElementById('status_tagihan');

            if (selectPenjualan && selectPelanggan) {
                selectPenjualan.addEventListener('change', function() {
                    const selectedOption = this.options[this.selectedIndex];
                    const idPelangganTerkait = selectedOption.getAttribute('data-pelanggan');
                    const sisaTagihanAktif = selectedOption.getAttribute('data-tagihan');

                    if (idPelangganTerkait) {
                        selectPelanggan.value = idPelangganTerkait;
                        inputSisaTagihan.value = sisaTagihanAktif; // Otomatis isi angka sisa tagihan asli
                        inputNominalBayar.max = sisaTagihanAktif; // Batasi agar tidak bayar melebihi sisa utang
                        inputNominalBayar.value = ""; // Reset input nominal bayar

                        selectPelanggan.classList.add('bg-slate-100', 'cursor-not-allowed');
                        selectPelanggan.style.pointerEvents = "none";
                    } else {
                        selectPelanggan.value = "";
                        inputSisaTagihan.value = "";
                        inputNominalBayar.value = "";
                        selectPelanggan.classList.remove('bg-slate-100', 'cursor-not-allowed');
                        selectPelanggan.style.pointerEvents = "auto";
                    }
                });

                // Otomatis ubah status ke 'Lunas' jika nominal bayar sama dengan sisa tagihan
                if (inputNominalBayar) {
                    inputNominalBayar.addEventListener('input', function() {
                        const sisa = parseFloat(inputSisaTagihan.value) || 0;
                        const bayar = parseFloat(this.value) || 0;

                        if (bayar >= sisa && sisa > 0) {
                            statusTagihan.value = "Lunas";
                        } else {
                            statusTagihan.value = "Belum Lunas";
                        }
                    });
                }
            }
            const btnPiutang = document.getElementById('btnPiutang');
            const modalPiutang = document.getElementById('modalPiutang');
            const closePiutang = document.getElementById('closePiutang');

            btnPiutang.addEventListener('click', () => {
                modalPiutang.classList.remove('hidden');
                document.body.classList.add('overflow-hidden');
            });

            closePiutang.addEventListener('click', () => {
                modalPiutang.classList.add('hidden');
                document.body.classList.remove('overflow-hidden');
            });


            const btnUtang = document.getElementById('btnUtang');
            const modalUtang = document.getElementById('modalUtang');
            const closeUtang = document.getElementById('closeUtang');

            btnUtang.addEventListener('click', () => {
                modalUtang.classList.remove('hidden');
                document.body.classList.add('overflow-hidden');
            });

            closeUtang.addEventListener('click', () => {
                modalUtang.classList.add('hidden');
                document.body.classList.remove('overflow-hidden');
            });

        });
    </script>
</body>

</html>