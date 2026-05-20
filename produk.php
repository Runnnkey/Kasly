<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit(); 
}

require_once 'koneksi.php'; 

$user_id = $_SESSION['user_id'];

$queryUser  = "SELECT * FROM user WHERE id_user = '$user_id'";
$resultUser = mysqli_query($conn, $queryUser);

if (mysqli_num_rows($resultUser) === 0) {
    session_destroy();
    header("Location: login.php");
    exit();
}

$rowUser = mysqli_fetch_assoc($resultUser);
$inisial = strtoupper(substr($rowUser['nama_lengkap'], 0, 1));

$id_umkm_user = $rowUser['id_umkm'];

$queryProduk = "SELECT * FROM produk WHERE id_umkm = ?";
$stmtProduk = mysqli_prepare($conn, $queryProduk);

if ($stmtProduk) {
    mysqli_stmt_bind_param($stmtProduk, "s", $id_umkm_user);
    mysqli_stmt_execute($stmtProduk);
    $resultProduk = mysqli_stmt_get_result($stmtProduk);
} else {
    $resultProduk = false;
}
?>


<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kasly - Inventaris</title>
    <link rel="stylesheet" href="dist/output.css">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;700&display=swap');
        body { font-family: 'Plus Jakarta Sans', sans-serif; }
    </style>
</head>
<body class="bg-slate-50/40">

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
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer"><a href="index.php" class="block p-3 w-full h-full">Dashboard</a></li>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer"><a href="transaksi.php" class="block p-3 w-full h-full">Transaksi</a></li>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer bg-indigo-50 text-indigo-600"><a href="produk.php" class="block p-3 w-full h-full">Produk</a></li>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer"><a href="utangPiutang.php" class="block p-3 w-full h-full">Utang & Piutang</a></li>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer"><a href="laporan.php" class="block p-3 w-full h-full">Laporan</a></li>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer"><a href="pengaturan.php" class="block p-3 w-full h-full">Pengaturan</a></li>
            <li class="hover:bg-red-50 text-red-600 rounded-lg cursor-pointer transition-colors">
                <a href="logout.php" class="block p-3 w-full h-full flex items-center gap-2">
                    <i class="fa-solid fa-right-from-bracket"></i>
                    <span>Keluar</span>
                </a>
            </li>
        </ul>
    </div>

    <section id="produk-page" class="space-y-6 p-4 md:p-6 max-w-7xl mx-auto">
        
        <div class="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <div>
                <h2 class="text-2xl font-black text-slate-800 tracking-tight">Data Barang & Inventaris</h2>
                <p class="text-sm text-slate-500 font-medium">Kelola stok produk secara real-time dari database.</p>
            </div>
        </div>

        <?php if (isset($_GET['status']) && $_GET['status'] == 'sukses_tambah'): ?>
            <div class="p-4 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-2xl text-xs font-semibold flex items-center gap-2 transition-all">
                <i class="fa-solid fa-circle-check text-base"></i> Produk baru berhasil ditambahkan!
            </div>
        <?php endif; ?>

        <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
            <div class="flex items-center gap-3 mb-5 border-b border-slate-100 pb-3">
                <div class="w-9 h-9 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center">
                    <i class="fa-solid fa-box-open text-base"></i>
                </div>
                <div>
                    <h3 class="text-sm font-black text-slate-800 tracking-tight">Form Tambah Produk Baru</h3>
                </div>
            </div>

            <form action="proses_tambah_produk.php" method="POST" class="space-y-4">
                <input type="hidden" name="id_umkm" value="<?php echo $id_umkm_user; ?>">

                <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div class="space-y-1.5">
                        <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Nama Produk <span class="text-red-500">*</span></label>
                        <input type="text" name="nama_produk" required placeholder="Masukan Nama Produk" 
                            class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                    </div>
                    
                    <div class="space-y-1.5">
                        <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Kategori <span class="text-red-500">*</span></label>
                        <input type="text" name="kategori" required placeholder="Contoh: Makanan, Minuman, Buah" 
                            class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                    </div>

                    <div class="space-y-1.5">
                        <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Stok Awal (Unit) <span class="text-red-500">*</span></label>
                        <input type="number" name="sisa_stok" required min="0" placeholder="0" 
                            class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                    </div>
                </div>

                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div class="space-y-1.5">
                        <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Harga Beli / Modal (Rp) <span class="text-red-500">*</span></label>
                        <input type="number" name="harga_beli" required min="0" placeholder="Masukkan harga modal" 
                            class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                    </div>

                    <div class="space-y-1.5">
                        <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Harga Jual (Rp) <span class="text-red-500">*</span></label>
                        <input type="number" name="harga_jual" required min="0" placeholder="Masukkan harga jual" 
                            class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                    </div>
                </div>

                <div class="flex justify-end pt-2">
                    <button type="submit" name="btn_simpan_produk" 
                        class="w-full md:w-auto px-6 py-2.5 bg-indigo-600 text-white rounded-xl text-xs font-bold hover:bg-indigo-700 hover:scale-[1.01] transition-all flex items-center justify-center gap-2 shadow-md shadow-indigo-100">
                        <i class="fa-solid fa-floppy-disk"></i> Simpan
                    </button>
                </div>
            </form>
        </div>

        <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
            <div class="overflow-x-auto">
                <table class="w-full text-left">
                    <thead class="bg-slate-50/50 border-b border-slate-100">
                        <tr>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Produk</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Kategori</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest text-center">Stok & Health Bar</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Harga Jual</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Harga Beli</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest text-right">Aksi</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-50">
                        
                    <?php 
                    if ($resultProduk && mysqli_num_rows($resultProduk) > 0):
                        while($row = mysqli_fetch_assoc($resultProduk)): 
                            
                            $stok = $row['sisa_stok'];
                            $max_stok = 100; 
                            
                            $persen = ($stok / $max_stok) * 100;
                            if ($persen > 100) $persen = 100; 
                            
                            $bar_color = "bg-emerald-500";
                            $text_color = "text-emerald-600";
                            $status_label = "Aman";

                            if($stok <= 10) {
                                $bar_color = "bg-rose-500";
                                $text_color = "text-rose-600";
                                $status_label = "Kritis";
                            } elseif ($stok <= 30) {
                                $bar_color = "bg-amber-500";
                                $text_color = "text-amber-600";
                                $status_label = "Menipis";
                            }
                    ?>
                        
                        <tr class="hover:bg-slate-50/50 transition-colors group">
                            <td class="p-4">
                                <div class="flex items-center gap-3">
                                    <div class="w-10 h-10 bg-slate-100 rounded-xl flex items-center justify-center text-lg">
                                        <?php 
                                        if ($row['kategori'] == 'Minuman') {
                                            echo '🥤';
                                        } elseif ($row['kategori'] == 'Makanan') {
                                            echo '🍔';
                                        } else {
                                            echo '📦';
                                        }
                                        ?>
                                    </div>
                                    <div>
                                        <p class="text-xs font-bold text-slate-800"><?php echo htmlspecialchars($row['nama_produk']); ?></p>
                                        <span class="text-[9px] text-slate-400 uppercase">ID: <?php echo $row['id_produk']; ?></span>
                                    </div>
                                </div>
                            </td>
                            <td class="p-4 text-xs font-medium text-slate-500"><?php echo htmlspecialchars($row['kategori']); ?></td>
                            <td class="p-4">
                                <div class="flex flex-col gap-1.5 min-w-[120px]">
                                    <div class="flex justify-between items-center text-[10px] font-bold">
                                        <span class="<?php echo $text_color; ?> uppercase"><?php echo $status_label; ?></span>
                                        <span class="text-slate-700"><?php echo $stok; ?> Unit</span>
                                    </div>
                                    <div class="w-full bg-slate-100 h-1.5 rounded-full overflow-hidden">
                                        <div class="<?php echo $bar_color; ?> h-full rounded-full" style="width: <?php echo $persen; ?>%"></div>
                                    </div>
                                </div>
                            </td>
                            <td class="p-4 text-xs font-black text-slate-700">Rp <?php echo number_format($row['harga_jual'], 0, ',', '.'); ?></td>
                            <td class="p-4 text-xs font-black text-slate-700">Rp <?php echo number_format($row['harga_beli'], 0, ',', '.'); ?></td>
                            <td class="p-4">
                                <div class="flex items-center justify-end gap-2">
                                    <a href="edit.php?id=<?php echo $row['id_produk']; ?>" class="w-8 h-8 rounded-lg bg-slate-100 text-slate-600 hover:bg-indigo-600 hover:text-white transition-all flex items-center justify-center text-xs shadow-sm">
                                        <i class="fa-solid fa-pen"></i>
                                    </a>
                                    <button onclick="confirmDelete(<?php echo $row['id_produk']; ?>)" class="w-8 h-8 rounded-lg bg-slate-100 text-rose-600 hover:bg-rose-600 hover:text-white transition-all flex items-center justify-center text-xs shadow-sm">
                                        <i class="fa-solid fa-trash"></i>
                                    </button>
                                </div>
                            </td>
                        </tr>
                        
                    <?php 
                        endwhile; 
                    else:
                    ?>
                        <tr>
                            <td colspan="6" class="p-8 text-center text-xs font-medium text-slate-400 italic bg-slate-50/30">
                                Belum ada data produk terdaftar untuk unit usaha Anda.
                            </td>
                        </tr>
                    <?php endif; ?>

                    </tbody>
                </table>
            </div>

            <div class="p-4 bg-slate-50/50 border-t border-slate-100 flex flex-col sm:flex-row justify-between items-center gap-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">
                <div class="flex gap-2">
                    <button class="px-3 py-1 rounded bg-white border border-slate-200 hover:bg-slate-50 transition-all">Prev</button>
                    <button class="px-3 py-1 rounded bg-white border border-slate-200 hover:bg-slate-50 transition-all">Next</button>
                </div>
            </div>
        </div>
    </section>
<script src="src/js/script.js"></script>
</body>
</html>