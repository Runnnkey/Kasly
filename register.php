<?php
include 'koneksi.php';
session_start();

if (isset($_SESSION['user_id'])) {
    header("Location: index.php");
    exit();
}

$error = "";
$success = "";
$registered = false;

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    $nama_lengkap = mysqli_real_escape_string($conn, trim($_POST['nama_lengkap']));
    $username     = mysqli_real_escape_string($conn, trim($_POST['username']));
    $email        = mysqli_real_escape_string($conn, trim($_POST['email']));
    $no_hp        = mysqli_real_escape_string($conn, trim($_POST['no_hp']));
    $password     = $_POST['password'];
    $confirm_pass = $_POST['confirm_password'];

    if (empty($nama_lengkap) || empty($username) || empty($email) || empty($no_hp) || empty($password)) {
        $error = "Semua field wajib diisi!";
    } elseif ($password !== $confirm_pass) {
        $error = "Konfirmasi password tidak cocok!";
    } elseif (strlen($password) < 6) {
        $error = "Password minimal 6 karakter!";
    } else {
        $check = mysqli_query($conn, "SELECT * FROM user WHERE username = '$username' OR email = '$email' OR no_hp = '$no_hp'");
        if (mysqli_num_rows($check) > 0) {
            $error = "Username, Email, atau Nomor HP sudah terdaftar!";
        } else {
            $hashed_password = password_hash($password, PASSWORD_DEFAULT);
            
            $query = "INSERT INTO user (nama_lengkap, username, email, no_hp, password, role) 
                      VALUES ('$nama_lengkap', '$username', '$email', '$no_hp', '$hashed_password', 'kasir')";
            
            if (mysqli_query($conn, $query)) {
                $success = "Akun berhasil dibuat!";
                $registered = true;
            } else {
                $error = "Gagal mendaftar. Silakan coba lagi.";
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
    <title>Ayo Daftar - Kasly</title>
    <!-- Ganti jalur CSS dengan CDN Tailwind supaya pasti jalan -->
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css" rel="stylesheet">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');

        body {
            font-family: 'Plus Jakarta Sans', sans-serif;
            background: linear-gradient(135deg, #6b21a8 0%, #a855f7 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 30px 20px;
        }

        .register-card {
            background: white;
            border-radius: 24px;
            box-shadow: 0 25px 60px -15px rgba(0, 0, 0, 0.15);
            width: 100%;
            max-width: 460px;
            padding: 40px 32px;
        }
        /* Tambahkan gaya tambahan untuk memastikan tampilan sama */
        input, select {
            border-radius: 16px !important;
            border: 1px solid #d1d5db !important;
            padding: 1rem 1.25rem !important;
        }
        input:focus {
            outline: none;
            border-color: #9333ea !important;
        }
        button {
            border-radius: 16px !important;
            background: linear-gradient(to right, #7e22ce, #d946ef);
        }
    </style>
</head>
<body>

    <div class="register-card">
        <div class="text-center mb-10">
            <h1 class="text-3xl font-bold text-gray-800">Ayo Daftar</h1>
            <p class="text-gray-600 mt-2">Isi data dirimu dengan benar</p>
        </div>

        <?php if ($error): ?>
            <div class="bg-red-50 border border-red-200 text-red-600 p-4 rounded-2xl text-sm mb-6">
                <?= $error ?>
            </div>
        <?php endif; ?>

        <?php if ($success): ?>
            <div class="bg-green-50 border border-green-200 text-green-700 p-8 rounded-3xl text-center">
                <div class="text-5xl mb-4">🎉</div>
                <h3 class="font-semibold text-xl"><?= $success ?></h3>
                <p class="mt-2">Akun kamu sudah siap digunakan</p>
                <a href="login.php" class="mt-6 block bg-gradient-to-r from-purple-600 to-fuchsia-600 text-white py-4 rounded-2xl font-semibold">
                    SELANJUTNYA →
                </a>
            </div>
        <?php endif; ?>

        <?php if (!$registered): ?>
        <form method="POST" class="space-y-6">
            
            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Nomor Telepon <span class="text-red-500">*</span></label>
                <div class="flex gap-2">
                    <div class="flex items-center border border-gray-300 rounded-2xl px-4 bg-gray-50">
                        <span class="text-xl">🇮🇩</span>
                            <option>+62</option>
                        </select>
                    </div>
                    <input type="text" name="no_hp" required
                           class="flex-1 border border-gray-300 rounded-2xl px-5 py-4 focus:outline-none focus:border-purple-500"
                           placeholder="8123456789">
                </div>
            </div>

            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Email <span class="text-red-500">*</span></label>
                <input type="email" name="email" required class="w-full border border-gray-300 rounded-2xl px-5 py-4 focus:outline-none focus:border-purple-500" placeholder="contoh@email.com">
            </div>

            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Nama Lengkap <span class="text-red-500">*</span></label>
                <input type="text" name="nama_lengkap" required class="w-full border border-gray-300 rounded-2xl px-5 py-4 focus:outline-none focus:border-purple-500" placeholder="Nama lengkap Anda">
            </div>

            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Username <span class="text-red-500">*</span></label>
                <input type="text" name="username" required class="w-full border border-gray-300 rounded-2xl px-5 py-4 focus:outline-none focus:border-purple-500" placeholder="Buat username unik">
            </div>

            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Kata Sandi <span class="text-red-500">*</span></label>
                <input type="password" name="password" required class="w-full border border-gray-300 rounded-2xl px-5 py-4 focus:outline-none focus:border-purple-500" placeholder="Minimal 6 karakter">
            </div>

            <div>
                <label class="block text-sm font-medium text-gray-700 mb-1">Konfirmasi Kata Sandi <span class="text-red-500">*</span></label>
                <input type="password" name="confirm_password" required class="w-full border border-gray-300 rounded-2xl px-5 py-4 focus:outline-none focus:border-purple-500" placeholder="Ulangi kata sandi">
            </div>

            <div class="flex items-start gap-3 pt-3">
                <input type="checkbox" required class="mt-1 accent-purple-600">
                <span class="text-sm text-gray-600">
                    Saya telah membaca dan menyetujui 
                    <a href="#" class="text-purple-600 hover:underline">Syarat dan Ketentuan</a>
                </span>
            </div>

            <button type="submit"
                    class="w-full bg-gradient-to-r from-purple-600 to-fuchsia-600 hover:from-purple-700 hover:to-fuchsia-700 text-white py-5 rounded-2xl font-bold text-xl shadow-lg transition-all">
                DAFTAR
            </button>
        </form>

        <div class="text-center mt-8 text-sm text-gray-600">
            Sudah punya akun? 
            <a href="login.php" class="text-purple-600 font-semibold hover:underline">Masuk Sekarang</a>
        </div>
        <?php endif; ?>
    </div>

</body>
</html>