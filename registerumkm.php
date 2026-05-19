<?php
include 'koneksi.php';
session_start();
$error = "";
$success = "";

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    $nama_usaha = mysqli_real_escape_string($conn, trim($_POST['nama_usaha']));
    $bidang_usaha = mysqli_real_escape_string($conn, trim($_POST['bidang_usaha']));
    $alamat = mysqli_real_escape_string($conn, trim($_POST['alamat']));
    $no_telepon = mysqli_real_escape_string($conn, trim($_POST['no_telepon']));
    $email = mysqli_real_escape_string($conn, trim($_POST['email'] ?? ''));

    if (empty($nama_usaha) || empty($bidang_usaha) || empty($alamat) || empty($no_telepon)) {
        $error = "Semua field wajib (*) harus diisi!";
    } else {
        $query = "INSERT INTO ms_umkm (nama_usaha, bidang_usaha, alamat, no_telepon, email)
                  VALUES ('$nama_usaha', '$bidang_usaha', '$alamat', '$no_telepon', '$email')";
       
        if (mysqli_query($conn, $query)) {
            $id_umkm_baru = mysqli_insert_id($conn);
            $success = "UMKM berhasil didaftarkan! ID UMKM: <strong>$id_umkm_baru</strong>";
        } else {
            $error = "Gagal mendaftar: " . mysqli_error($conn);
        }
    }
}
?>

<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Registrasi UMKM - Kasly</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@500;600;700&display=swap" rel="stylesheet">
    
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    fontFamily: { sans: ['Plus Jakarta Sans', 'sans-serif'] }
                }
            }
        }
    </script>
</head>
<body class="bg-gradient-to-br from-blue-900 to-indigo-900 min-h-screen flex items-center justify-center p-4">
    <div class="bg-white w-full max-w-5xl rounded-3xl shadow-2xl overflow-hidden flex">
        
        <!-- Bagian Kiri - Visual -->
        <div class="hidden lg:flex w-1/2 bg-gradient-to-br from-indigo-50 to-violet-50 items-center justify-center p-12 relative">
            <div class="text-center">
                <!-- Logo -->
                <img src="Assets/LogoBaru.png" alt="Kasly Logo" 
                     class="w-64 mx-auto mb-8 drop-shadow-xl">

                <!-- Teks di bawah logo (sudah dikecilkan) -->
                <h2 class="text-4xl font-bold text-gray-900 mb-2">Daftarkan Usaha Anda</h2>
                <p class="text-indigo-600 text-2xl font-semibold mb-4">Bersama Kasly</p>
                <p class="text-gray-600 text-base max-w-xs mx-auto leading-relaxed">
                    Kelola UMKM Anda dengan mudah, cepat,<br>dan profesional
                </p>
            </div>
        </div>

        <!-- Bagian Kanan - Form -->
        <div class="w-full lg:w-1/2 p-10 md:p-14">
            <div class="max-w-md mx-auto">
                <!-- Logo Mobile -->
                <div class="lg:hidden text-center mb-8">
                    <img src="Assets/LogoBaru.png" alt="Kasly Logo" class="w-20 h-20 mx-auto">
                </div>

                <h1 class="text-3xl font-bold text-gray-900 text-center">Registrasi UMKM</h1>
                <p class="text-gray-500 text-center mt-2 mb-10">Daftarkan usaha Anda ke platform Kasly</p>

                <?php if ($error): ?>
                    <div class="bg-red-50 border border-red-200 text-red-600 p-4 rounded-2xl mb-6 text-center">
                        <?= $error ?>
                    </div>
                <?php endif; ?>

                <?php if ($success): ?>
                    <div class="bg-emerald-50 border border-emerald-200 text-emerald-700 p-10 rounded-3xl text-center">
                        <i class="fa-solid fa-circle-check text-6xl mb-6"></i>
                        <p class="font-bold text-2xl"><?= $success ?></p>
                        <a href="register.php" class="mt-8 block bg-gradient-to-r from-indigo-600 to-blue-600 text-white py-4 rounded-2xl font-semibold hover:brightness-110 transition">
                            Buat Akun User →
                        </a>
                    </div>
                <?php else: ?>

                <form action="" method="POST" class="space-y-6">
                    <div>
                        <label class="block text-sm font-semibold text-gray-700 mb-2">Nama Usaha <span class="text-red-500">*</span></label>
                        <input type="text" name="nama_usaha" required class="w-full px-5 py-4 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-indigo-100 outline-none transition">
                    </div>

                    <div>
                        <label class="block text-sm font-semibold text-gray-700 mb-2">Bidang / Jenis Usaha <span class="text-red-500">*</span></label>
                        <select name="bidang_usaha" required class="w-full px-5 py-4 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-indigo-100 outline-none transition">
                            <option value="">Pilih Bidang Usaha</option>
                            <option value="Makanan & Minuman">Makanan & Minuman</option>
                            <option value="Fashion">Fashion</option>
                            <option value="Kerajinan Tangan">Kerajinan Tangan</option>
                            <option value="Pertanian">Pertanian / Peternakan</option>
                            <option value="Jasa">Jasa</option>
                            <option value="Retail">Retail</option>
                            <option value="Lainnya">Lainnya</option>
                        </select>
                    </div>

                    <div>
                        <label class="block text-sm font-semibold text-gray-700 mb-2">Alamat Lengkap <span class="text-red-500">*</span></label>
                        <textarea name="alamat" rows="3" required class="w-full px-5 py-4 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-indigo-100 outline-none transition resize-y"></textarea>
                    </div>

                    <div class="grid grid-cols-2 gap-5">
                        <div>
                            <label class="block text-sm font-semibold text-gray-700 mb-2">No. Telepon / WA <span class="text-red-500">*</span></label>
                            <input type="tel" name="no_telepon" required class="w-full px-5 py-4 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-indigo-100 outline-none transition">
                        </div>
                        <div>
                            <label class="block text-sm font-semibold text-gray-700 mb-2">Email (Opsional)</label>
                            <input type="email" name="email" class="w-full px-5 py-4 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-indigo-100 outline-none transition">
                        </div>
                    </div>

                    <button type="submit" 
                            class="w-full bg-gradient-to-r from-indigo-600 to-blue-600 hover:from-indigo-700 hover:to-blue-700 text-white font-bold py-5 rounded-2xl text-lg transition-all shadow-lg mt-4">
                        DAFTARKAN UMKM
                    </button>
                </form>

                <?php endif; ?>

                <div class="text-center mt-8">
                    <a href="login.php" class="text-indigo-600 hover:text-indigo-700 font-medium">← Kembali ke Login</a>
                </div>
            </div>
        </div>
    </div>
</body>
</html>