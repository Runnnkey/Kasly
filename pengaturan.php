<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pengaturan</title>
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
    <li class="hover:bg-red-50 text-red-600 rounded-lg cursor-pointer transition-colors">
        <a href="logout.php" class="block p-3 w-full h-full flex items-center gap-2">
            <i class="fa-solid fa-right-from-bracket"></i>
            <span>Keluar</span>
        </a>
    </li>
        </ul>
</div>
<section id="pengaturan-page" class="space-y-6">
    <div class="flex flex-col lg:flex-row gap-8">
        
        <!-- SIDEBAR MENU KIRI -->
        <div class="w-full lg:w-72 space-y-2">
            <button class="w-full text-left px-5 py-4 bg-indigo-600 text-white rounded-2xl font-bold text-sm flex items-center gap-3 shadow-lg shadow-indigo-100 transition-all">
                <i class="fa-solid fa-user w-5"></i> Profil User
            </button>
            <button class="w-full text-left px-5 py-4 text-slate-500 hover:bg-slate-50 rounded-2xl font-bold text-sm flex items-center gap-3 transition">
                <i class="fa-solid fa-store w-5"></i> Profil UMKM & Struk
            </button>
            <button class="w-full text-left px-5 py-4 text-slate-500 hover:bg-slate-50 rounded-2xl font-bold text-sm flex items-center gap-3 transition">
                <i class="fa-solid fa-users-gear w-5"></i> Manajemen User
            </button>
            <button class="w-full text-left px-5 py-4 text-slate-500 hover:bg-slate-50 rounded-2xl font-bold text-sm flex items-center gap-3 transition">
                <i class="fa-solid fa-sliders w-5"></i> Preferensi Aplikasi
            </button>
            <button class="w-full text-left px-5 py-4 text-slate-500 hover:bg-slate-50 rounded-2xl font-bold text-sm flex items-center gap-3 transition">
                <i class="fa-solid fa-shield w-5"></i> Keamanan & Data
            </button>
        </div>

        <!-- AREA KONTEN UTAMA (KANAN) -->
        <div class="flex-1 space-y-8">
            
            <!-- 1. KARTU PROFIL PENGGUNA (YANG BARU) -->
            <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
                <div class="p-6 border-b border-slate-100 bg-slate-50/50 flex justify-between items-center">
                    <div>
                        <h3 class="font-bold text-slate-800">Profil Pengguna</h3>
                        <p class="text-[11px] text-slate-400 font-medium">Kelola identitas personal, hak akses, dan pantau log aktivitas Anda.</p>
                    </div>
                    <span class="text-[10px] bg-indigo-50 text-indigo-600 px-3 py-1 rounded-full font-black uppercase tracking-wider">Akun Aktif</span>
                </div>

                <div class="p-8 space-y-8">
                    <!-- Foto & Identitas Dasar -->
    <div class="flex flex-col sm:flex-row sm:items-start gap-8 pb-6 border-b border-slate-100">

    <!-- Avatar -->
    <div class="relative w-24 h-24 bg-indigo-100 border border-indigo-200 rounded-full flex items-center justify-center text-indigo-600 font-black text-3xl shadow-inner group cursor-pointer overflow-hidden shrink-0">
        <span class="group-hover:opacity-0 transition-opacity">R</span>

        <div class="absolute inset-0 bg-black/40 opacity-0 group-hover:opacity-100 transition-opacity flex flex-col items-center justify-center text-white text-[10px] font-bold">
            <i class="fa-solid fa-camera text-sm mb-1"></i>
            UBAH FOTO
        </div>
    </div>

    <!-- Form -->
    <div class="grid grid-cols-1 md:grid-cols-2 gap-6 flex-1">

        <!-- Nama -->
        <div class="space-y-2">
            <label class="text-xs font-black text-slate-400 uppercase tracking-wider">Nama Lengkap</label>
            <input type="text" value="Runn" class="w-full px-5 py-4 bg-slate-50 border border-slate-200 rounded-2xl focus:ring-2 focus:ring-indigo-500 outline-none text-base font-semibold text-slate-700">
        </div>

        <!-- Email -->
        <div class="space-y-2">
            <label class="text-xs font-black text-slate-400 uppercase tracking-wider">
                Alamat Email
            </label>

            <input 
                type="email" 
                value="runn@kasly.id"
                class="w-full px-5 py-4 bg-slate-50 border border-slate-200 rounded-2xl focus:ring-2 focus:ring-indigo-500 outline-none text-base font-semibold text-slate-700"
            >
        </div>

    </div>
</div>

                    <!-- Otoritas & Hak Akses -->
                    <div class="space-y-3">
                        <h4 class="text-xs font-black text-slate-700 uppercase tracking-widest flex items-center gap-2">
                            <i class="fa-solid fa-user-lock text-indigo-600"></i> Otoritas & Hak Akses Toko
                        </h4>
                        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                            <div class="p-4 bg-slate-50/80 border border-slate-200/60 rounded-2xl select-none">
                                <p class="text-[9px] font-black text-slate-400 uppercase tracking-wider mb-1">Level Jabatan</p>
                                <div class="text-xs font-bold text-slate-700 flex items-center gap-1.5">
                                    <i class="fa-solid fa-user-shield text-indigo-600"></i> Owner (Pemilik Utama)
                                </div>
                            </div>
                            <div class="p-4 bg-slate-50/80 border border-slate-200/60 rounded-2xl select-none">
                                <p class="text-[9px] font-black text-slate-400 uppercase tracking-wider mb-1">Terdaftar Sejak</p>
                                <div class="text-xs font-bold text-slate-700 flex items-center gap-1.5">
                                    <i class="fa-regular fa-calendar-check text-slate-500"></i> 08 Juli 2025
                                </div>
                            </div>
                            <div class="p-4 bg-slate-50/80 border border-slate-200/60 rounded-2xl select-none">
                                <p class="text-[9px] font-black text-slate-400 uppercase tracking-wider mb-1">Status Sesi / Shift</p>
                                <div class="text-xs font-bold text-emerald-600 flex items-center gap-1.5">
                                    <i class="fa-solid fa-circle text-[6px] animate-pulse"></i> Bebas Akses (No-Limit)
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- Log Aktivitas & Perangkat -->
                    <div class="space-y-3 pt-2">
                        <h4 class="text-xs font-black text-slate-700 uppercase tracking-widest flex items-center gap-2">
                            <i class="fa-solid fa-clock-rotate-left text-indigo-600"></i> Log Aktivitas & Perangkat
                        </h4>
                        <div class="border border-slate-100 rounded-2xl overflow-hidden divide-y divide-slate-50">
                            <div class="p-3.5 flex items-center justify-between hover:bg-slate-50/50 transition-colors">
                                <div class="flex items-center gap-3">
                                    <div class="text-slate-400"><i class="fa-solid fa-laptop text-base"></i></div>
                                    <div>
                                        <p class="text-xs font-bold text-slate-700">Perangkat Saat Ini</p>
                                        <p class="text-[10px] text-slate-400">Ubuntu Linux • Sesi Aktif</p>
                                    </div>
                                </div>
                                <span class="text-[10px] font-bold text-emerald-600 bg-emerald-50 px-2 py-0.5 rounded-md">Online</span>
                            </div>
                            <div class="p-3.5 flex items-center justify-between hover:bg-slate-50/50 transition-colors">
                                <div class="flex items-center gap-3">
                                    <div class="text-slate-400"><i class="fa-solid fa-receipt text-base"></i></div>
                                    <div>
                                        <p class="text-xs font-bold text-slate-700">Aksi Terakhir Sistem</p>
                                        <p class="text-[10px] text-slate-400">Mengekspor Laporan Bulanan ke Excel</p>
                                    </div>
                                </div>
                                <span class="text-[10px] font-medium text-slate-400 italic">5 mnt yang lalu</span>
                            </div>
                        </div>
                    </div>

                    <!-- Tombol Simpan -->
                    <div class="flex justify-end pt-2">
                        <button class="w-full md:w-auto px-6 py-2.5 bg-indigo-600 text-white rounded-xl text-xs font-bold hover:bg-indigo-700 hover:scale-[1.02] transition-all shadow-md shadow-indigo-100 flex items-center justify-center gap-2">
                            <i class="fa-solid fa-floppy-disk"></i> Simpan Perubahan Profil
                        </button>
                    </div>
                </div>
            </div>
            
            <!-- 2. KARTU PROFIL USAHA & STRUK -->
            <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
                <div class="p-6 border-b border-slate-100 bg-slate-50/50">
                    <h3 class="font-bold text-slate-800">Profil Usaha & Struk</h3>
                </div>
                <div class="p-8 space-y-6">
                    <div class="flex items-center gap-6 pb-6 border-b border-slate-100">
                        <div class="w-24 h-24 bg-slate-50 border-2 border-dashed border-slate-200 rounded-2xl flex flex-col items-center justify-center text-slate-400 hover:border-indigo-400 hover:text-indigo-600 cursor-pointer transition-all group">
                            <i class="fa-solid fa-image text-xl mb-1 group-hover:scale-110 transition"></i>
                            <span class="text-[9px] font-black uppercase">Upload Logo</span>
                        </div>
                        <div>
                            <h4 class="font-bold text-slate-800">Logo Usaha</h4>
                            <p class="text-xs text-slate-400">Muncul di dashboard dan cetak struk.</p>
                        </div>
                    </div>

                    <div class="grid grid-cols-1 md:grid-cols-2 gap-6">
                        <div class="space-y-2">
                            <label class="text-xs font-black text-slate-500 uppercase">Nama Toko</label>
                            <input type="text" value="Avokita Shop" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium">
                        </div>
                        <div class="space-y-2">
                            <label class="text-xs font-black text-slate-500 uppercase">WhatsApp Toko</label>
                            <input type="text" value="081234567890" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium">
                        </div>
                        <div class="space-y-2 md:col-span-2">
                            <label class="text-xs font-black text-slate-500 uppercase">Header Struk (Pesan Pembuka)</label>
                            <input type="text" placeholder="Contoh: Fresh from the farm!" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium">
                        </div>
                        <div class="space-y-2 md:col-span-2">
                            <label class="text-xs font-black text-slate-500 uppercase">Footer Struk (Pesan Penutup)</label>
                            <textarea placeholder="Contoh: Terima kasih sudah berbelanja, Runn!" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium" rows="2"></textarea>
                        </div>
                    </div>
                </div>
            </div>

            <!-- 3. PREFERENSI & VISUAL -->
            <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
                <div class="p-6 border-b border-slate-100 bg-slate-50/50">
                    <h3 class="font-bold text-slate-800">Preferensi & Visual</h3>
                </div>
                <div class="p-8 grid grid-cols-1 md:grid-cols-2 gap-8">
                    <div class="flex items-center justify-between">
                        <div>
                            <p class="font-bold text-slate-700 text-sm">Mata Uang</p>
                            <p class="text-xs text-slate-400">Format angka yang digunakan.</p>
                        </div>
                        <select class="p-2 bg-slate-100 border-none rounded-lg text-xs font-bold outline-none cursor-pointer">
                            <option>Rupiah (IDR)</option>
                            <option>Dollar (USD)</option>
                        </select>
                    </div>
                    <div class="flex items-center justify-between">
                        <div>
                            <p class="font-bold text-slate-700 text-sm">Mode Gelap</p>
                            <p class="text-xs text-slate-400">Ubah tampilan jadi lebih gelap.</p>
                        </div>
                        <div class="w-12 h-6 bg-slate-200 rounded-full relative cursor-pointer shadow-inner">
                            <div class="absolute left-1 top-1 w-4 h-4 bg-white rounded-full shadow transition-all"></div>
                        </div>
                    </div>
                    <div class="flex items-center justify-between md:col-span-2 border-t pt-6 border-slate-50">
                        <div>
                            <p class="font-bold text-slate-700 text-sm">Notifikasi Peringatan Stok</p>
                            <p class="text-xs text-slate-400">Ingatkan jika stok produk di bawah 5.</p>
                        </div>
                        <div class="w-12 h-6 bg-indigo-600 rounded-full relative cursor-pointer">
                            <div class="absolute right-1 top-1 w-4 h-4 bg-white rounded-full shadow transition-all"></div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- 4. KEAMANAN & DATA -->
            <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
                <div class="p-6 border-b border-slate-100 bg-slate-50/50 flex justify-between items-center">
                    <h3 class="font-bold text-slate-800">Keamanan Akun & Kedaulatan Data</h3>
                    <span class="text-[10px] bg-emerald-100 text-emerald-600 px-2 py-1 rounded-md font-bold italic">Tingkat Keamanan: Tinggi</span>
                </div>
                
                <div class="p-8 space-y-8">
                    <div class="grid grid-cols-1 md:grid-cols-2 gap-6 pb-8 border-b border-slate-50">
                        <div class="space-y-1">
                            <h4 class="text-sm font-bold text-slate-800">Ganti Kata Sandi</h4>
                            <p class="text-xs text-slate-400">Pastikan password kamu kuat dan sulit ditebak.</p>
                        </div>
                        <div class="space-y-3">
                            <input type="password" placeholder="Password Lama" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl text-xs outline-none focus:ring-2 focus:ring-indigo-500">
                            <input type="password" placeholder="Password Baru" class="w-full p-3 bg-slate-50 border border-slate-200 rounded-xl text-xs outline-none focus:ring-2 focus:ring-indigo-500">
                            <button class="w-full py-2.5 bg-indigo-50 text-indigo-600 rounded-xl text-xs font-bold hover:bg-indigo-600 hover:text-white transition">Update Password</button>
                        </div>
                    </div>

                </div>
            </div>

        </div>
    </div>
</section>
<script src="src/js/script.js"></script>

</body>
</html>