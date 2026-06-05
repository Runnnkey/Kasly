<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

$user_id = $_SESSION['user_id'];

$queryUser = "SELECT id_user, id_umkm, nama_lengkap FROM user WHERE id_user = '$user_id'";
$resultUser = mysqli_query($conn, $queryUser);

if (mysqli_num_rows($resultUser) === 0) {
    session_destroy();
    header("Location: login.php");
    exit();
}

$rowUser = mysqli_fetch_assoc($resultUser);
$inisial = strtoupper(substr($rowUser['nama_lengkap'], 0, 1));
$id_umkm = $rowUser['id_umkm'];

// Ambil ID produk dari URL
$id_produk = isset($_GET['id']) ? $_GET['id'] : 0;

// Ambil data produk
$queryProduk = "SELECT * FROM produk WHERE id_produk = '$id_produk' AND id_umkm = '$id_umkm'";
$resultProduk = mysqli_query($conn, $queryProduk);

if (mysqli_num_rows($resultProduk) === 0) {
    header("Location: produk.php");
    exit();
}

$produk = mysqli_fetch_assoc($resultProduk);

// Proses update data
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $nama_produk = $_POST['nama_produk'];
    $kategori = $_POST['kategori'];
    $harga_jual = $_POST['harga_jual'];
    $harga_beli = $_POST['harga_beli'];
    $sisa_stok = $_POST['sisa_stok'];

    $queryUpdate = "UPDATE produk SET 
                    nama_produk = '$nama_produk',
                    kategori = '$kategori',
                    harga_jual = '$harga_jual',
                    harga_beli = '$harga_beli',
                    sisa_stok = '$sisa_stok'
                    WHERE id_produk = '$id_produk' AND id_umkm = '$id_umkm'";

    if (mysqli_query($conn, $queryUpdate)) {
        header("Location: produk.php?status=edit_sukses");
        exit();
    } else {
        $error = "Gagal mengupdate produk: " . mysqli_error($conn);
    }
}
?>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Edit Produk - Kasly</title>
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
                <img src="Assets/LogoBaru.png" alt="Kasly Logo" class="w-12 h-12 object-contain">
            </div>
        </div>
        <div class="flex items-center gap-4">
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

    <section class="space-y-8 p-4 md:p-6 max-w-2xl mx-auto">
        <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
            <div class="flex items-center gap-3 mb-6 border-b border-slate-100 pb-4">
                <div class="w-10 h-10 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center">
                    <i class="fa-solid fa-pen text-base"></i>
                </div>
                <div>
                    <h3 class="text-base font-black text-slate-800 tracking-tight">Edit Produk</h3>
                    <p class="text-xs text-slate-400 font-medium">Ubah data produk yang sudah ada.</p>
                </div>
            </div>

            <?php if (isset($error)): ?>
                <div class="p-3 mb-4 bg-rose-50 border border-rose-200 text-rose-700 rounded-xl text-xs">
                    <?php echo $error; ?>
                </div>
            <?php endif; ?>

            <form action="" method="POST" class="space-y-4">
                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div class="space-y-1.5">
                        <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Nama Produk <span class="text-red-500">*</span></label>
                        <input type="text" name="nama_produk" required value="<?php echo htmlspecialchars($produk['nama_produk']); ?>"
                            class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                    </div>
                    
                    <div class="space-y-1.5">
                        <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Kategori <span class="text-red-500">*</span></label>
                        <input type="text" name="kategori" required value="<?php echo htmlspecialchars($produk['kategori']); ?>"
                            class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                    </div>
                </div>

                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div class="space-y-1.5">
                        <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Stok (Unit) <span class="text-red-500">*</span></label>
                        <input type="number" name="sisa_stok" required min="0" value="<?php echo $produk['sisa_stok']; ?>"
                            class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                    </div>

                    <div class="space-y-1.5">
                        <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Harga Beli / Modal (Rp) <span class="text-red-500">*</span></label>
                        <input type="number" name="harga_beli" required min="0" value="<?php echo $produk['harga_beli']; ?>"
                            class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                    </div>
                </div>

                <div class="space-y-1.5">
                    <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Harga Jual (Rp) <span class="text-red-500">*</span></label>
                    <input type="number" name="harga_jual" required min="0" value="<?php echo $produk['harga_jual']; ?>"
                        class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-xs font-medium text-slate-700">
                </div>

                <div class="flex justify-end gap-3 pt-2">
                    <a href="produk.php" class="px-6 py-2.5 bg-slate-200 text-slate-700 rounded-xl text-xs font-bold hover:bg-slate-300 transition-all">
                        Batal
                    </a>
                    <button type="submit" class="px-6 py-2.5 bg-indigo-600 text-white rounded-xl text-xs font-bold hover:bg-indigo-700 transition-all flex items-center gap-2 shadow-md">
                        <i class="fa-solid fa-save"></i> Simpan Perubahan
                    </button>
                </div>
            </form>
        </div>
    </section>

    <script src="src/js/script.js"></script>
</body>
</html>