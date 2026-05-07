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
    
    <!-- Header & Action Buttons -->
    <div class="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-2">
        <div>
            <h2 class="text-2xl font-black text-slate-800 tracking-tight">Data Barang & Inventaris</h2>
            <p class="text-sm text-slate-500 font-medium">Kelola stok produk Avokita dan pantau aset gudang secara akurat.</p>
        </div>
        <div class="flex items-center gap-2">
            <button class="bg-indigo-600 text-white px-5 py-2.5 rounded-xl text-xs font-bold hover:bg-indigo-700 hover:scale-105 transition-all flex items-center gap-2 shadow-lg shadow-indigo-100">
                <i class="fa-solid fa-plus"></i> Tambah Produk
            </button>
        </div>
    </div>

    <!-- 1. Status Cards (Ringkasan Cepat) -->
    <div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <div class="bg-white p-5 rounded-3xl border border-slate-200 shadow-sm group hover:border-indigo-100 transition-all">
            <div class="flex items-center gap-3 mb-3">
                <div class="w-10 h-10 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center shadow-inner">
                    <i class="fa-solid fa-boxes-stacked"></i>
                </div>
                <span class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Total SKU</span>
            </div>
            <h3 class="text-2xl font-black text-slate-800">24 <span class="text-xs font-medium text-slate-400 ml-1">Varian</span></h3>
        </div>

        <div class="bg-rose-50 p-5 rounded-3xl border border-rose-100 shadow-sm relative overflow-hidden">
            <div class="flex items-center gap-3 mb-3">
                <div class="w-10 h-10 bg-white text-rose-600 rounded-xl flex items-center justify-center shadow-sm">
                    <i class="fa-solid fa-triangle-exclamation"></i>
                </div>
                <span class="text-[10px] font-bold text-rose-400 uppercase tracking-widest">Stok Kritis</span>
            </div>
            <h3 class="text-2xl font-black text-rose-600">3 <span class="text-xs font-medium text-rose-400 ml-1">Perlu Restok</span></h3>
        </div>

        <div class="bg-white p-5 rounded-3xl border border-slate-200 shadow-sm group hover:border-emerald-100 transition-all">
            <div class="flex items-center gap-3 mb-3">
                <div class="w-10 h-10 bg-emerald-50 text-emerald-600 rounded-xl flex items-center justify-center shadow-inner">
                    <i class="fa-solid fa-money-bill-trend-up"></i>
                </div>
                <span class="text-[10px] font-bold text-slate-400 uppercase tracking-widest">Nilai Aset</span>
            </div>
            <h3 class="text-2xl font-black text-slate-800">Rp 12.450k</h3>
        </div>

        <div class="bg-indigo-600 p-5 rounded-3xl shadow-xl shadow-indigo-100 text-white">
            <div class="flex items-center gap-3 mb-3">
                <div class="w-10 h-10 bg-white/20 text-white rounded-xl flex items-center justify-center">
                    <i class="fa-solid fa-cart-shopping"></i>
                </div>
                <span class="text-[10px] font-bold text-indigo-100 uppercase tracking-widest">Keluar (7 Hari)</span>
            </div>
            <h3 class="text-2xl font-black">156 <span class="text-xs font-medium text-indigo-200 ml-1">Kg</span></h3>
        </div>
    </div>

    <!-- 2. Kategori Visual (Tabs/Chips) -->
    <div class="flex items-center gap-2 overflow-x-auto pb-2 no-scrollbar">
        <button class="bg-indigo-600 text-white px-5 py-2 rounded-full text-xs font-bold shadow-md shadow-indigo-100 flex items-center gap-2 shrink-0">
            <i class="fa-solid fa-layer-group"></i> Semua
        </button>
        <button class="bg-white border border-slate-200 text-slate-600 px-5 py-2 rounded-full text-xs font-bold hover:bg-slate-50 transition-all flex items-center gap-2 shrink-0">
            <i class="fa-solid fa-seedling text-emerald-500"></i> Alpukat
        </button>
        <button class="bg-white border border-slate-200 text-slate-600 px-5 py-2 rounded-full text-xs font-bold hover:bg-slate-50 transition-all flex items-center gap-2 shrink-0">
            <i class="fa-solid fa-lemon text-amber-500"></i> Buah Tropis
        </button>
        <button class="bg-white border border-slate-200 text-slate-600 px-5 py-2 rounded-full text-xs font-bold hover:bg-slate-50 transition-all flex items-center gap-2 shrink-0">
            <i class="fa-solid fa-box text-blue-500"></i> Kemasan
        </button>
    </div>

    <!-- Tabel Produk -->
    <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
        <div class="overflow-x-auto">
            <table class="w-full text-left">
                <thead class="bg-slate-50/50 border-b border-slate-100">
                    <tr>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Produk</th>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Kategori</th>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest text-center">Stok & Health Bar</th>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">Harga Jual</th>
                        <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest text-right">Aksi</th>
                    </tr>
                </thead>
                <tbody class="divide-y divide-slate-50">
                    <!-- Item 1: Stok Melimpah -->
                    <tr class="hover:bg-slate-50/50 transition-colors group">
                        <td class="p-4">
                            <div class="flex items-center gap-3">
                                <div class="w-10 h-10 bg-slate-100 rounded-xl flex items-center justify-center text-lg">🥑</div>
                                <div>
                                    <p class="text-xs font-bold text-slate-800">Alpukat Mentega Super</p>
                                    <span class="bg-indigo-50 text-indigo-600 text-[9px] font-bold px-2 py-0.5 rounded-full uppercase">Terlaris</span>
                                </div>
                            </div>
                        </td>
                        <td class="p-4 text-xs font-medium text-slate-500">Alpukat</td>
                        <td class="p-4">
                            <!-- 3. Indikator Visual Health Bar -->
                            <div class="flex flex-col gap-1.5 min-w-[120px]">
                                <div class="flex justify-between items-center text-[10px] font-bold">
                                    <span class="text-emerald-600 uppercase">Aman</span>
                                    <span class="text-slate-700">85 / 100 kg</span>
                                </div>
                                <div class="w-full bg-slate-100 h-1.5 rounded-full overflow-hidden">
                                    <div class="bg-emerald-500 h-full w-[85%] rounded-full"></div>
                                </div>
                            </div>
                        </td>
                        <td class="p-4 text-xs font-black text-slate-700">Rp 35.000</td>
                        <td class="p-4">
                            <!-- 4. Quick Action Buttons -->
                            <div class="flex items-center justify-end gap-2">
                                <button class="w-8 h-8 rounded-lg bg-slate-100 text-slate-600 hover:bg-indigo-600 hover:text-white transition-all flex items-center justify-center text-xs shadow-sm">
                                    <i class="fa-solid fa-minus"></i>
                                </button>
                                <button class="w-8 h-8 rounded-lg bg-slate-100 text-slate-600 hover:bg-indigo-600 hover:text-white transition-all flex items-center justify-center text-xs shadow-sm">
                                    <i class="fa-solid fa-plus"></i>
                                </button>
                                <button class="w-8 h-8 rounded-lg bg-white border border-slate-200 text-slate-400 hover:text-indigo-600 transition-all flex items-center justify-center text-xs">
                                    <i class="fa-solid fa-ellipsis-vertical"></i>
                                </button>
                            </div>
                        </td>
                    </tr>

                    <!-- Item 2: Stok Kritis -->
                    <tr class="hover:bg-slate-50/50 transition-colors group">
                        <td class="p-4">
                            <div class="flex items-center gap-3">
                                <div class="w-10 h-10 bg-slate-100 rounded-xl flex items-center justify-center text-lg">📦</div>
                                <div>
                                    <p class="text-xs font-bold text-slate-800">Kardus Avokita (L)</p>
                                    <span class="bg-rose-50 text-rose-600 text-[9px] font-bold px-2 py-0.5 rounded-full uppercase tracking-tighter">Stok Tipis</span>
                                </div>
                            </div>
                        </td>
                        <td class="p-4 text-xs font-medium text-slate-500">Kemasan</td>
                        <td class="p-4">
                            <div class="flex flex-col gap-1.5">
                                <div class="flex justify-between items-center text-[10px] font-bold">
                                    <span class="text-rose-600 uppercase italic">Kritis</span>
                                    <span class="text-slate-700">12 / 100 pcs</span>
                                </div>
                                <div class="w-full bg-slate-100 h-1.5 rounded-full overflow-hidden">
                                    <div class="bg-rose-500 h-full w-[12%] rounded-full"></div>
                                </div>
                            </div>
                        </td>
                        <td class="p-4 text-xs font-black text-slate-700">Rp 5.000</td>
                        <td class="p-4 text-right">
                             <div class="flex items-center justify-end gap-2">
                                <button class="w-8 h-8 rounded-lg bg-slate-100 text-slate-600 hover:bg-indigo-600 hover:text-white transition-all flex items-center justify-center text-xs shadow-sm"><i class="fa-solid fa-minus"></i></button>
                                <button class="w-8 h-8 rounded-lg bg-slate-100 text-slate-600 hover:bg-indigo-600 hover:text-white transition-all flex items-center justify-center text-xs shadow-sm"><i class="fa-solid fa-plus"></i></button>
                                <button class="w-8 h-8 rounded-lg bg-white border border-slate-200 text-slate-400 hover:text-indigo-600 transition-all flex items-center justify-center text-xs"><i class="fa-solid fa-ellipsis-vertical"></i></button>
                            </div>
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
        <!-- Footer Tabel -->
        <div class="p-4 bg-slate-50/50 border-t border-slate-100 flex flex-col sm:flex-row justify-between items-center gap-4 text-[10px] font-bold text-slate-400 uppercase tracking-widest">
            <p>Menampilkan 2 dari 24 Produk</p>
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