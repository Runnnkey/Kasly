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
        if (password_verify($password, $user['password'])) {
            $_SESSION['user_id'] = $user['id_user'];
            $_SESSION['username'] = $user['username'];
            $_SESSION['id_umkm'] = $user['id_umkm']; 
            $_SESSION['nama_lengkap'] = $user['nama_lengkap'];
            $_SESSION['role'] = $user['role'];

            header("Location: index.php");
            exit();
        } else {
            $error = "Username atau password salah!";
        }
    } else {
        $error = "Username tidak terdaftar!";
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
<body class="bg-gradient-to-br from-[#1E3A8A] via-[#312E81] to-[#4F46E5] min-h-screen flex items-center justify-center p-4">

    <div class="bg-white w-full max-w-5xl rounded-3xl shadow-2xl overflow-hidden flex">

        <!-- Bagian Kiri -->
        <div class="hidden lg:flex w-1/2 bg-gradient-to-br from-blue-50 to-indigo-100 items-center justify-center p-12">
            <div class="text-center">
                <img src="Assets/LogoBaru.png" alt="Kasly Logo" class="w-72 mx-auto mb-6">
                
                <h2 class="text-4xl font-bold text-gray-800 tracking-tight">Kelola UMKM</h2>
                <p class="text-indigo-600 text-xl font-semibold mt-1">Lebih Mudah dengan Kasly</p>
            </div>
        </div>

        <!-- Bagian Kanan - Form -->
        <div class="w-full lg:w-1/2 p-10 flex flex-col justify-center bg-white">
            <div class="max-w-md mx-auto w-full">
                
                <div class="lg:hidden text-center mb-8">
                    <img src="Assets/LogoBaru.png" alt="Kasly Logo" class="w-24 h-24 mx-auto">
                </div>

                <h1 class="text-3xl font-bold text-center text-gray-900">Login</h1>
                <p class="text-center text-gray-500 mt-2 mb-10">Selamat Datang Kembali</p>

                <?php if ($error): ?>
                    <div class="bg-red-50 border border-red-200 text-red-600 p-4 rounded-2xl mb-6 text-center">
                        <?= $error ?>
                    </div>
                <?php endif; ?>

                <form method="POST" class="space-y-6">
                    <div>
                        <input type="text" name="username" required autofocus
                               class="w-full px-6 py-5 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-4 focus:ring-indigo-100 outline-none transition text-base"
                               placeholder="Username">
                    </div>

                    <div class="relative">
                        <input type="password" name="password" id="password" required
                               class="w-full px-6 py-5 rounded-2xl border border-gray-200 focus:border-indigo-500 focus:ring-4 focus:ring-indigo-100 outline-none transition text-base"
                               placeholder="Kata Sandi">
                        <button type="button" onclick="togglePassword()" 
                                class="absolute right-6 top-1/2 -translate-y-1/2 text-gray-400 hover:text-indigo-600">
                            <i class="fa-solid fa-eye text-xl" id="eye-icon"></i>
                        </button>
                    </div>

                    <button type="submit"
                            class="w-full bg-gradient-to-r from-indigo-600 to-blue-600 hover:from-indigo-700 hover:to-blue-700 text-white font-bold py-5 rounded-2xl text-lg transition-all active:scale-[0.97] shadow-lg shadow-indigo-500/30">
                        MASUK
                    </button>
                </form>

                <div class="text-center mt-8">
                    <span class="text-gray-500">Belum punya akun?</span>
                    <a href="register.php" class="text-indigo-600 font-semibold hover:underline ml-1">Registrasi Sekarang</a>
                    <br>
                    <span class="text-gray-500">Punya bisnis?</span>
                    <a href="registerumkm.php" class="text-indigo-600 font-semibold hover:underline ml-1">Daftar UMKM</a>
                </div>
            </div>
        </div>
    </div>

    <script>
        function togglePassword() {
            const pwd = document.getElementById('password');
            const icon = document.getElementById('eye-icon');
            if (pwd.type === "password") {
                pwd.type = "text";
                icon.classList.replace('fa-eye', 'fa-eye-slash');
            } else {
                pwd.type = "password";
                icon.classList.replace('fa-eye-slash', 'fa-eye');
            }
        }
    </script>
</body>
</html>