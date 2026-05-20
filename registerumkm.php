<?php
include 'koneksi.php';
session_start();

$error = "";
$success = "";
$step = 1; // Default step 1: Registrasi UMKM
$id_umkm_baru = 0;

// Menangkap id_umkm_baru dari session atau post jika terjadi reload halaman akibat error validasi
if (isset($_POST['id_umkm'])) {
    $id_umkm_baru = intval($_POST['id_umkm']);
}

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    
    // LOGIKA PROSES 1: DAFTAR UMKM
    if (isset($_POST['btn_daftar_umkm'])) {
        $nama_usaha = mysqli_real_escape_string($conn, trim($_POST['nama_usaha']));
        $bidang_usaha = mysqli_real_escape_string($conn, trim($_POST['bidang_usaha']));
        $alamat = mysqli_real_escape_string($conn, trim($_POST['alamat']));
        $no_telepon = mysqli_real_escape_string($conn, trim($_POST['no_telepon']));

        if (empty($nama_usaha) || empty($bidang_usaha) || empty($alamat) || empty($no_telepon)) {
            $error = "Semua field wajib (*) pada data usaha harus diisi!";
        } else {
            $query = "INSERT INTO ms_umkm (nama_usaha, bidang_usaha, alamat, no_telepon)
                      VALUES ('$nama_usaha', '$bidang_usaha', '$alamat', '$no_telepon')";
           
            if (mysqli_query($conn, $query)) {
                $id_umkm_baru = mysqli_insert_id($conn);
                $step = 2; // Berhasil! Lompatkan tampilan ke form pembuatan akun user
            } else {
                $error = "Gagal mendaftar usaha: " . mysqli_error($conn);
            }
        }
    }
    
    // LOGIKA PROSES 2: BUAT AKUN USER (DIPICU SETELAH STEP 1 SELESAI)
    if (isset($_POST['btn_buat_user'])) {
        $step = 2; // Kunci form agar tetap berada di tampilan pembuatan user jika ada error validasi
        $nama_lengkap = mysqli_real_escape_string($conn, trim($_POST['nama_lengkap']));
        $username     = mysqli_real_escape_string($conn, trim($_POST['username']));
        $password     = $_POST['password'];
        $role         = $_POST['role'];
        $id_umkm      = mysqli_real_escape_string($conn, $_POST['id_umkm']); // Menangkap nilai hidden input

        if (empty($nama_lengkap) || empty($username) || empty($password) || empty($id_umkm) || empty($role)) {
            $error = "Semua field pembuatan akun wajib diisi!";
        } elseif (strlen($password) < 5) {
            $error = "Password minimal terdiri dari 5 karakter!";
        } else {
            $check = mysqli_query($conn, "SELECT * FROM user WHERE username = '$username'");
            if (mysqli_num_rows($check) > 0) {
                $error = "Username tersebut sudah terdaftar! Gunakan username lain.";
            } else {
                $hashed_password = password_hash($password, PASSWORD_DEFAULT);
                $query_user = "INSERT INTO user (id_umkm, nama_lengkap, username, password, role) 
                               VALUES ('$id_umkm', '$nama_lengkap', '$username', '$hashed_password', '$role')";
                
                if (mysqli_query($conn, $query_user)) {
                    $success = "Akun Pengguna Berhasil Dibuat! Silakan masuk ke aplikasi.";
                    $step = 3; // Menampilkan screen sukses akhir
                } else {
                    $error = "Gagal membuat akun user: " . mysqli_error($conn);
                }
            }
        }
    }
}
?>

<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Registrasi Sistem - Kasly</title>
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
        
        <div class="hidden lg:flex w-1/2 bg-gradient-to-br from-indigo-50 to-violet-50 items-center justify-center p-12 relative">
            <div class="text-center">
                <img src="Assets/LogoBaru.png" alt="Kasly Logo" class="w-64 mx-auto mb-8 drop-shadow-xl">
                <h2 class="text-4xl font-bold text-gray-900 mb-2">Daftarkan Usaha Anda</h2>
                <p class="text-indigo-600 text-2xl font-semibold mb-4">Bersama Kasly</p>
                <p class="text-gray-600 text-base max-w-xs mx-auto leading-relaxed">
                    Kelola UMKM Anda dengan mudah, cepat,<br>dan profesional
                </p>
            </div>
        </div>

        <div class="w-full lg:w-1/2 p-10 md:p-14">
            <div class="max-w-md mx-auto">
                <div class="lg:hidden text-center mb-8">
                    <img src="Assets/LogoBaru.png" alt="Kasly Logo" class="w-20 h-20 mx-auto">
                </div>

                <?php if ($step === 3): ?>
                    <div class="bg-emerald-50 border border-emerald-200 text-emerald-700 p-8 rounded-3xl text-center">
                        <i class="fa-solid fa-circle-check text-6xl text-emerald-500 mb-6"></i>
                        <h2 class="font-bold text-2xl text-gray-900 mb-2">Pendaftaran Sukses!</h2>
                        <p class="text-sm text-gray-600 mb-6"><?= $success ?></p>
                        <a href="login.php" class="block w-full text-center bg-gradient-to-r from-indigo-600 to-blue-600 text-white py-4 rounded-2xl font-semibold hover:brightness-110 transition shadow-lg shadow-indigo-100">
                            Masuk ke Aplikasi Kasly →
                        </a>
                    </div>
                <?php else: ?>

                    <h1 class="text-3xl font-bold text-gray-900 text-center">
                        <?= $step === 1 ? 'Registrasi UMKM' : 'Buat Akun Owner' ?>
                    </h1>
                    <p class="text-gray-500 text-center mt-2 mb-10">
                        <?= $step === 1 ? 'Langkah 1: Daftarkan badan usaha Anda ke platform Kasly' : 'Langkah 2: Buat kredensial login pemilik utama toko' ?>
                    </p>

                    <?php if ($error): ?>
                        <div class="bg-red-50 border border-red-200 text-red-600 p-4 rounded-2xl mb-6 text-center font-medium text-sm">
                            <i class="fa-solid fa-triangle-exclamation mr-1"></i> <?= $error ?>
                        </div>
                    <?php endif; ?>

                    <?php if ($step === 1): ?>
                        <form action="" method="POST" class="space-y-6">
                            <div>
                                <label class="block text-sm font-semibold text-gray-700 mb-2">Nama Usaha <span class="text-red-500">*</span></label>
                                <input type="text" name="nama_usaha" required class="w-full px-5 py-4 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-indigo-100 outline-none transition" placeholder="Masukkan Nama Usaha">
                            </div>

                            <div>
                                <label class="block text-sm font-semibold text-gray-700 mb-2">Bidang / Jenis Usaha <span class="text-red-500">*</span></label>
                                <select name="bidang_usaha" required class="w-full px-5 py-4 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-indigo-100 outline-none transition bg-white text-gray-700">
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
                                <textarea name="alamat" rows="3" required class="w-full px-5 py-4 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-indigo-100 outline-none transition resize-y" placeholder="Masukkan Alamat Usaha"></textarea>
                            </div>

                            <div>
                                <label class="block text-sm font-semibold text-gray-700 mb-2">No. Telepon / WA <span class="text-red-500">*</span></label>
                                <input type="tel" name="no_telepon" required class="w-full px-5 py-4 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-indigo-100 outline-none transition" placeholder="Masukkan Nomor Telepon">
                            </div>

                            <button type="submit" name="btn_daftar_umkm" class="w-full bg-gradient-to-r from-indigo-600 to-blue-600 hover:from-indigo-700 hover:to-blue-700 text-white font-bold py-5 rounded-2xl text-lg transition-all shadow-lg mt-4">
                                LANJUT PENDAFTARAN
                            </button>
                        </form>

                    <?php elseif ($step === 2): ?>
                        <form action="" method="POST" class="space-y-5">
                            
                            <input type="hidden" name="id_umkm" value="<?php echo $id_umkm_baru; ?>">

                            <div>
                                <label class="block mb-2 text-sm font-semibold text-gray-700">Nama Lengkap <span class="text-red-500">*</span></label>
                                <input type="text" name="nama_lengkap" required class="w-full px-5 py-3.5 rounded-2xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-indigo-500/20 focus:border-indigo-500 transition-all text-sm" placeholder="Masukkan Nama Lengkap">
                            </div>

                            <div>
                                <label class="block mb-2 text-sm font-semibold text-gray-700">Username <span class="text-red-500">*</span></label>
                                <input type="text" name="username" required class="w-full px-5 py-3.5 rounded-2xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-indigo-500/20 focus:border-indigo-500 transition-all text-sm" placeholder="Masukan Username">
                            </div>

                            <div>
                                <label class="block mb-2 text-sm font-semibold text-gray-700">Password <span class="text-red-500">*</span></label>
                                <input type="password" name="password" required class="w-full px-5 py-3.5 rounded-2xl border border-gray-200 focus:outline-none focus:ring-2 focus:ring-indigo-500/20 focus:border-indigo-500 transition-all text-sm" placeholder="Masukan Kata Sandi">
                            </div>

                            <div>
                                <label class="block mb-2 text-sm font-semibold text-gray-700">Role Jabatan <span class="text-red-500">*</span></label>
                                <input type="text" name="role" value="Owner" readonly required class="w-full px-5 py-3.5 rounded-2xl border border-gray-200 bg-slate-100 text-slate-500 cursor-not-allowed outline-none text-sm font-bold">
                            </div>

                            <button type="submit" name="btn_buat_user" class="w-full bg-gradient-to-r from-indigo-600 to-blue-600 hover:from-indigo-700 hover:to-blue-700 text-white font-bold py-4 rounded-2xl text-base transition-all shadow-lg mt-4">
                                DAFTAR UMKM
                            </button>
                        </form>
                    <?php endif; ?>

                <?php endif; ?>

                <div class="text-center mt-8">
                    <a href="login.php" class="text-indigo-600 hover:text-indigo-700 font-medium text-sm">← Kembali ke Login</a>
                </div>
            </div>
        </div>
    </div>
</body>
</html>