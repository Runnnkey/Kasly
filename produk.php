<?php
include 'koneksi.php';

$query = "SELECT * FROM produk";
$result = mysqli_query($conn, $query);


?>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Kasly</title>
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
        <div class="w-8 h-8 bg-indigo-600 rounded-lg flex items-center justify-center text-white font-bold">K</div>
        <span class="font-bold text-xl tracking-tight text-indigo-600">Kasly</span>
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
        </ul>
</div>

    <section id="produk-page" class="space-y-8 p-4 md:p-6">
        
        <div class="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-2">
            <div>
                <h2 class="text-2xl font-black text-slate-800 tracking-tight">Data Barang & Inventaris</h2>
                <p class="text-sm text-slate-500 font-medium">Kelola stok produk secara real-time dari database.</p>
            </div>
            <div class="flex items-center gap-2">
                <button class="bg-indigo-600 text-white px-5 py-2.5 rounded-xl text-xs font-bold hover:bg-indigo-700 transition-all flex items-center gap-2 shadow-lg shadow-indigo-100">
                    <i class="fa-solid fa-plus"></i> Tambah Produk
                </button>
            </div>
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
                        while($row = mysqli_fetch_assoc($result)): 
                            // Logika sederhana untuk Health Bar
                            $stok = $row['sisa_stok'];
                            $max_stok = 100; // Contoh batas max untuk persentase bar
                            $persen = ($stok / $max_stok) * 100;
                            
                            // Penentuan warna berdasarkan jumlah stok
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
                                        <?php echo ($row['kategori'] == 'Minuman') ? '🥤' : '🥑'; ?>
                                    </div>
                                    <div>
                                        <p class="text-xs font-bold text-slate-800"><?php echo $row['nama_produk']; ?></p>
                                        <span class="text-[9px] text-slate-400 uppercase">ID: <?php echo $row['id_produk']; ?></span>
                                    </div>
                                </div>
                            </td>
                            <td class="p-4 text-xs font-medium text-slate-500"><?php echo $row['kategori']; ?></td>
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
                        <?php endwhile; ?>

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