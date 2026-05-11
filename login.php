<?php
include 'koneksi.php';
session_start();

if (isset($_SESSION['user_id'])) {
    header("Location: index.php");
    exit();
}

$error = "";
if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    $username = mysqli_real_escape_string($conn, trim($_POST['username']));
    $password = $_POST['password'];

    $query = "SELECT * FROM user WHERE username = '$username'";
    $result = mysqli_query($conn, $query);

    if ($result && mysqli_num_rows($result) > 0) {
        $user = mysqli_fetch_assoc($result);
        if (password_verify($password, $user['password']) || $user['password'] === $password) {
            $_SESSION['user_id'] = $user['id_user'];
            $_SESSION['username'] = $user['username'];
            $_SESSION['nama_lengkap'] = $user['nama_lengkap'] ?? $user['username'];
            $_SESSION['role'] = $user['role'] ?? 'kasir';

            header("Location: index.php");
            exit();
        } else {
            $error = "Username atau password salah!";
        }
    } else {
        $error = "Username tidak ditemukan!";
    }
}
?>

<!DOCTYPE html>
<html lang="id">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Masuk ke Kasly</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css" rel="stylesheet">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap');

        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        body {
            font-family: 'Plus Jakarta Sans', sans-serif;
            background: linear-gradient(135deg, #6b21a8 0%, #a855f7 100%);
            min-height: 100vh;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 30px 20px;
        }

        .login-card {
            background: #ffffff;
            border-radius: 24px;
            box-shadow: 0 25px 60px -15px rgba(0, 0, 0, 0.15);
            width: 100%;
            max-width: 460px;
            padding: 40px 32px;
        }

        /* ✅ INPUT: BULAT PENUH, TEPAT UKURAN */
        input {
            border-radius: 9999px !important;
            border: 1px solid #d1d5db !important;
            padding: 0 1.25rem !important;
            font-size: 0.95rem !important;
            height: 46px !important;
            line-height: 46px !important;
            transition: all 0.2s ease;
            background-color: #fff !important;
            color: #1f2937 !important;
        }
        input::placeholder {
            color: #9ca3af !important;
        }
        input:focus {
            outline: none;
            border-color: #d1d5db !important; /* Tetap abu, ungu cuma bayangan */
            box-shadow: 0 0 0 2px rgba(147, 51, 234, 0.15) !important;
        }

        /* ✅ TOMBOL MASUK */
        button[type="submit"] {
            border-radius: 9999px !important;
            background: linear-gradient(to right, #7e22ce, #d946ef) !important;
            color: #ffffff !important;
            transition: all 0.3s ease;
            height: 48px !important;
            border: none !important;
            font-weight: bold !important;
            font-size: 1.05rem !important;
        }
        button[type="submit"]:hover {
            transform: translateY(-2px);
            box-shadow: 0 10px 20px rgba(126, 34, 206, 0.2);
        }

        /* ✅ WARNA TEKS */
        h1 { color: #111827 !important; font-weight: 700 !important; font-size: 1.875rem !important; }
        .tagline { color: #9333ea !important; font-weight: 600 !important; font-size: 1.1rem !important; }
        h2 { color: #1f2937 !important; font-weight: 600 !important; font-size: 1.25rem !important; }
        .desc { color: #6b7280 !important; font-size: 0.95rem !important; }
        label { color: #374151 !important; font-weight: 500 !important; font-size: 0.9rem !important; }
        .link-purple { color: #9333ea !important; text-decoration: none !important; }
        .link-purple:hover { text-decoration: underline !important; }
        .text-gray { color: #6b7280 !important; font-size: 0.9rem !important; }
    </style>
</head>
<body>

    <div class="login-card">
        <div class="text-center mb-10">
            <h1>Kasly</h1>
            <p class="tagline mt-1">#Manajemen UMKM Cerdas</p>
            <h2 class="mt-6">Masuk ke Kasly</h2>
            <p class="desc mt-2">Selamat datang kembali 👋</p>
        </div>

        <?php if ($error): ?>
            <div class="bg-red-50 border border-red-200 text-red-600 p-4 rounded-2xl text-sm mb-6 text-center">
                <?= $error ?>
            </div>
        <?php endif; ?>

        <form method="POST" class="space-y-5">
            <div>
                <label class="block mb-2">Username / Email</label>
                <input type="text" name="username" required autofocus
                       class="w-full"
                       placeholder="Masukkan username atau email Anda">
            </div>

            <div class="relative">
                <label class="block mb-2">Kata Sandi</label>
                <input type="password" name="password" id="password" required
                       class="w-full"
                       placeholder="Masukkan kata sandi">
                <button type="button" onclick="togglePassword()" 
                        class="absolute right-4 top-1/2 -translate-y-2/2 bg-transparent border-0 p-0 m-0 shadow-none w-5 h-5 flex items-center justify-center">
                    <i class="fa-solid fa-eye text-gray-400 text-sm" id="eye-icon"></i>
                </button>
            </div>

            <div class="flex items-center justify-between text-sm mt-2">
                <label class="flex items-center gap-2 cursor-pointer">
                    <input type="checkbox" class="w-4 h-4 accent-purple-600 rounded">
                    <span class="text-gray">Ingat saya</span>
                </label>
                <a href="#" class="link-purple font-medium">Lupa kata sandi?</a>
            </div>

            <button type="submit"
                    class="w-full shadow-md mt-4">
                MASUK
            </button>
        </form>

        <div class="text-center mt-8">
            <span class="text-gray">Belum punya akun?</span> 
            <a href="register.php" class="link-purple font-semibold">Daftar Sekarang</a>
        </div>
    </div>

    <script>
        function togglePassword() {
            const pwd = document.getElementById('password');
            const icon = document.getElementById('eye-icon');
            
            if (pwd.type === "password") {
                pwd.type = "text";
                icon.classList.replace('fa-eye', 'fa-eye-slash');
                icon.classList.remove('text-purple-600');
                icon.classList.add('text-gray-400');
            } else {
                pwd.type = "password";
                icon.classList.replace('fa-eye-slash', 'fa-eye');
                icon.classList.remove('text-purple-600');
                icon.classList.add('text-gray-400');
            }
        }
    </script>
</body>
</html>