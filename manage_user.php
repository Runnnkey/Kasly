<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

$user_id = $_SESSION['user_id'];

// Cek apakah user adalah Owner
$queryUser = "SELECT role, nama_lengkap, id_umkm FROM user WHERE id_user = '$user_id'";
$resultUser = mysqli_query($conn, $queryUser);
$user = mysqli_fetch_assoc($resultUser);

if ($user['role'] !== 'Owner') {
    $_SESSION['error'] = "Akses ditolak! Hanya Owner yang bisa mengelola user.";
    header("Location: index.php");
    exit();
}

$id_umkm = $user['id_umkm'];
$inisial = strtoupper(substr($user['nama_lengkap'], 0, 1));

// Ambil semua user dari UMKM yang sama (kecuali Owner)
$queryUsers = "SELECT * FROM user WHERE id_umkm = '$id_umkm' AND role != 'Owner' ORDER BY role, nama_lengkap";
$resultUsers = mysqli_query($conn, $queryUsers);

// Notifikasi
$notif = '';
if (isset($_GET['status'])) {
    if ($_GET['status'] == 'edit_sukses') {
        $notif = '<div class="p-4 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-2xl text-xs font-semibold flex items-center gap-2"><i class="fa-solid fa-circle-check"></i> User berhasil diperbarui!</div>';
    } elseif ($_GET['status'] == 'hapus_sukses') {
        $notif = '<div class="p-4 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-2xl text-xs font-semibold flex items-center gap-2"><i class="fa-solid fa-circle-check"></i> User berhasil dihapus!</div>';
    } elseif ($_GET['status'] == 'error') {
        $notif = '<div class="p-4 bg-rose-50 border border-rose-200 text-rose-700 rounded-2xl text-xs font-semibold flex items-center gap-2"><i class="fa-solid fa-circle-exclamation"></i> ' . htmlspecialchars($_GET['msg']) . '</div>';
    }
}
?>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Manajemen User - Kasly</title>
    <link rel="stylesheet" href="dist/output.css">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" rel="stylesheet">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;600;700&display=swap');
        body { font-family: 'Plus Jakarta Sans', sans-serif; }
    </style>
</head>
<body class="bg-slate-50/40">

    <!-- NAVBAR -->
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
            <div class="w-10 h-10 rounded-full bg-indigo-100 border border-indigo-200 flex items-center justify-center text-indigo-600 font-bold">
                <?php echo $inisial; ?>
            </div>
        </div>
    </nav>

    <!-- SIDEBAR -->
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
            <!-- Dashboard -->
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="index.php" class="block p-3 w-full h-full">Dashboard</a>
            </li>

            <!-- Transaksi -->
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="transaksi.php" class="block p-3 w-full h-full">Transaksi</a>
            </li>

            <!-- Produk -->
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="produk.php" class="block p-3 w-full h-full">Produk</a>
            </li>

            <!-- Utang & Piutang (Kasir tidak bisa akses) -->
            <?php if ($user['role'] !== 'Kasir'): ?>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="utangPiutang.php" class="block p-3 w-full h-full">Utang & Piutang</a>
            </li>
            <?php endif; ?>

            <!-- Laporan (Kasir tidak bisa akses) -->
            <?php if ($user['role'] !== 'Kasir'): ?>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="laporan.php" class="block p-3 w-full h-full">Laporan</a>
            </li>
            <?php endif; ?>

            <!-- Manajemen User (Hanya Owner) -->
            <?php if ($user['role'] == 'Owner'): ?>
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer bg-indigo-50 text-indigo-600">
                <a href="manage_user.php" class="block p-3 w-full h-full flex items-center gap-2">Manajemen User</a>
            </li>
            <?php endif; ?>

            <!-- Pengaturan -->
            <li class="hover:bg-slate-100 rounded-lg cursor-pointer">
                <a href="pengaturan.php" class="block p-3 w-full h-full">Pengaturan</a>
            </li>

            <!-- Garis pemisah -->
            <hr class="border-slate-100 my-2">

            <!-- Keluar -->
            <li class="hover:bg-red-50 text-red-600 rounded-lg cursor-pointer transition-colors">
                <a href="logout.php" class="block p-3 w-full h-full flex items-center gap-2">
                    <i class="fa-solid fa-right-from-bracket"></i>
                    <span>Keluar</span>
                </a>
            </li>
        </ul>
    </div>

    <!-- MAIN CONTENT -->
    <section class="space-y-8 p-4 md:p-6 max-w-6xl mx-auto">
        
        <!-- HEADER -->
        <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
            <div class="flex flex-col md:flex-row md:items-center justify-between gap-6">
                <div class="flex items-center gap-4">
                    <div class="w-14 h-14 bg-indigo-100 text-indigo-600 rounded-2xl flex items-center justify-center shadow-inner">
                        <i class="fa-solid fa-users-gear text-2xl"></i>
                    </div>
                    <div>
                        <h2 class="text-2xl font-black text-slate-800 tracking-tight">Manajemen User</h2>
                        <p class="text-sm text-slate-500 font-medium">Kelola akun Admin & Kasir yang mengelola bisnis Anda.</p>
                    </div>
                </div>
            </div>
        </div>

        <!-- NOTIFIKASI -->
        <?php echo $notif; ?>

        <!-- DAFTAR USER -->
        <div class="bg-white border border-slate-200 rounded-3xl overflow-hidden shadow-sm">
            <div class="p-6 border-b border-slate-100 flex justify-between items-center bg-slate-50/50">
                <h3 class="font-bold text-slate-800 flex items-center gap-2 uppercase text-xs tracking-widest">
                    <i class="fa-solid fa-users text-indigo-500"></i> Daftar Karyawan
                </h3>
                <span class="text-xs text-slate-400">
                    <i class="fa-regular fa-user"></i> Total: <?php echo mysqli_num_rows($resultUsers); ?> user
                </span>
            </div>

            <div class="overflow-x-auto">
                <table class="w-full text-left border-collapse">
                    <thead class="bg-slate-50">
                        <tr>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">No</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Nama</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Username</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider">Role</th>
                            <th class="p-4 text-[10px] font-bold text-slate-400 uppercase tracking-wider text-center">Aksi</th>
                        </tr>
                    </thead>
                    <tbody class="divide-y divide-slate-100">
                        <?php 
                        $no = 1;
                        if (mysqli_num_rows($resultUsers) > 0):
                            while ($row = mysqli_fetch_assoc($resultUsers)):
                                $roleBadge = ($row['role'] == 'Admin') 
                                    ? 'bg-purple-100 text-purple-600' 
                                    : 'bg-emerald-100 text-emerald-600';
                        ?>
                        <tr class="hover:bg-slate-50 transition group">
                            <td class="p-4 text-sm text-slate-500 font-medium"><?php echo $no++; ?></td>
                            <td class="p-4 text-sm font-bold text-slate-700"><?php echo htmlspecialchars($row['nama_lengkap']); ?></td>
                            <td class="p-4 text-sm text-slate-600"><?php echo htmlspecialchars($row['username']); ?></td>
                            <td class="p-4">
                                <span class="<?php echo $roleBadge; ?> text-[10px] px-2 py-1 rounded-full font-bold uppercase">
                                    <?php echo $row['role']; ?>
                                </span>
                            </td>
                            <td class="p-4 text-center">
                                <div class="flex items-center justify-center gap-2">
                                    <a href="edit_user.php?id=<?php echo $row['id_user']; ?>" 
                                       class="w-8 h-8 rounded-lg bg-slate-100 text-slate-600 hover:bg-indigo-600 hover:text-white transition-all flex items-center justify-center text-xs shadow-sm"
                                       title="Edit User">
                                        <i class="fa-solid fa-pen"></i>
                                    </a>
                                    <button onclick="confirmDelete(<?php echo $row['id_user']; ?>)" 
                                            class="w-8 h-8 rounded-lg bg-slate-100 text-rose-600 hover:bg-rose-600 hover:text-white transition-all flex items-center justify-center text-xs shadow-sm"
                                            title="Hapus User">
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
                            <td colspan="5" class="p-8 text-center text-sm text-slate-400">
                                <i class="fa-regular fa-user text-2xl block mb-2 opacity-20"></i>
                                Belum ada user yang terdaftar.
                            </td>
                        </tr>
                        <?php endif; ?>
                    </tbody>
                </table>
            </div>

            <!-- FOOTER TABEL -->
            <div class="p-4 bg-slate-50/50 border-t border-slate-100 flex justify-between items-center text-[10px] font-bold text-slate-400 uppercase tracking-wider">
                <span>Menampilkan <?php echo mysqli_num_rows($resultUsers); ?> user</span>
            </div>
        </div>

    </section>

    <script src="src/js/script.js"></script>
    <script>
        // Konfirmasi Hapus
        function confirmDelete(id) {
            if (confirm('Apakah Anda yakin ingin menghapus user ini?')) {
                window.location.href = 'proses_hapus_user.php?id=' + id;
            }
        }
    </script>
</body>
</html>