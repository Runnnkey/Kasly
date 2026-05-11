<?php
include 'koneksi.php';
session_start();

// Jika sudah login, langsung arahkan ke dashboard
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
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    colors: {
                        brand: {
                            primary: '#6358ff', 
                            light: '#7c73ff',
                            dark: '#4f44e6',
                        }
                    },
                    fontFamily: {
                        sans: ['Plus Jakarta Sans', 'sans-serif'],
                    },
                }
            }
        }
    </script>
</head>
<body class="bg-gradient-to-br from-[#6358ff] to-[#8b83ff] min-h-screen flex items-center justify-center p-5 font-sans">

    <div class="bg-white w-full max-w-[460px] rounded-[24px] shadow-2xl p-8 md:p-10">
        <div class="text-center mb-10">
            <h1 class="text-3xl font-bold text-gray-900">Kasly</h1>
        </div>

        <?php if ($error): ?>
            <div class="bg-red-50 border border-red-200 text-red-600 p-4 rounded-2xl text-sm mb-6 text-center animate-pulse">
                <i class="fa-solid fa-circle-exclamation mr-2"></i><?= $error ?>
            </div>
        <?php endif; ?>

        <form method="POST" class="space-y-5">
            <div>
                <label class="block mb-2 text-sm font-medium text-gray-700">Username</label>
                <input type="text" name="username" required autofocus
                       class="w-full px-5 py-3 rounded-full border border-gray-300 focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all placeholder:text-gray-400"
                       placeholder="Masukkan username Anda">
            </div>

            <div class="relative">
                <label class="block mb-2 text-sm font-medium text-gray-700">Kata Sandi</label>
                <div class="relative">
                    <input type="password" name="password" id="password" required
                           class="w-full px-5 py-3 rounded-full border border-gray-300 focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all placeholder:text-gray-400"
                           placeholder="Masukkan kata sandi">
                    <button type="button" onclick="togglePassword()" 
                            class="absolute right-5 top-1/2 -translate-y-1/2 text-gray-400 hover:text-brand-primary transition-colors">
                        <i class="fa-solid fa-eye text-sm" id="eye-icon"></i>
                    </button>
                </div>
            </div>

            <button type="submit"
                    class="w-full bg-gradient-to-r from-brand-dark to-brand-primary text-white font-bold py-3.5 rounded-full shadow-lg shadow-brand-primary/20 hover:shadow-brand-primary/30 hover:-translate-y-0.5 transition-all active:scale-[0.98] mt-4 uppercase tracking-wider">
                MASUK
            </button>
        </form>

        <div class="text-center mt-10">
            <span class="text-gray-500 text-sm">Belum punya akun?</span> 
            <a href="register.php" class="text-brand-primary font-bold text-sm hover:underline ml-1">Daftar Sekarang</a>
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