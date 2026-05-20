<?php
include 'koneksi.php';
session_start();

if (isset($_SESSION['user_id'])) {
    header("Location: index.php");
    exit();
}

$query_umkm = "SELECT id_umkm, nama_usaha FROM ms_umkm ORDER BY nama_usaha ASC";
$result_umkm = mysqli_query($conn, $query_umkm);

$error = "";
$success = "";
$registered = false;

if ($_SERVER['REQUEST_METHOD'] == 'POST') {
    $nama_lengkap = mysqli_real_escape_string($conn, trim($_POST['nama_lengkap']));
    $username     = mysqli_real_escape_string($conn, trim($_POST['username']));
    $password     = $_POST['password'];
    $role         = $_POST['role'];
    $id_umkm      = mysqli_real_escape_string($conn, $_POST['id_umkm']); 

    if (empty($nama_lengkap) || empty($username) || empty($password) || empty($id_umkm) || empty($role)) {
        $error = "Semua field wajib diisi!";
    } elseif (strlen($password) < 5) {
        $error = "Password minimal 5 karakter!";
    } else {
        $check = mysqli_query($conn, "SELECT * FROM user WHERE username = '$username'");
        
        if (mysqli_num_rows($check) > 0) {
            $error = "Username sudah terdaftar!";
        } else {
            $hashed_password = password_hash($password, PASSWORD_DEFAULT);
            
            $query = "INSERT INTO user (id_umkm, nama_lengkap, username, password, role) 
                      VALUES ('$id_umkm', '$nama_lengkap', '$username', '$hashed_password', '$role')";
            
            if (mysqli_query($conn, $query)) {
                $success = "Akun berhasil dibuat!";
                $registered = true;
            } else {
                $error = "Gagal mendaftar. Error: " . mysqli_error($conn);
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
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.0/css/all.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script>
        tailwind.config = {
            theme: {
                extend: {
                    colors: { brand: { primary: '#6358ff', light: '#7c73ff', dark: '#4f44e6' } },
                    fontFamily: { sans: ['Plus Jakarta Sans', 'sans-serif'] }
                }
            }
        }
    </script>
</head>
<body class="bg-gradient-to-br from-[#6358ff] to-[#8b83ff] min-h-screen flex items-center justify-center p-5 font-sans">

    <div class="bg-white w-full max-w-[480px] rounded-[32px] shadow-2xl p-8 md:p-10 my-10">
        <div class="text-center mb-10">
            <h1 class="text-3xl font-bold text-gray-900">Register</h1>
        </div>

        <?php if ($error): ?>
            <div class="bg-red-50 border border-red-200 text-red-600 p-4 rounded-2xl text-sm mb-6 flex items-center gap-3">
                <i class="fa-solid fa-circle-exclamation"></i> <?= $error ?>
            </div>
        <?php endif; ?>

        <?php if ($success): ?>
            <div class="bg-white text-center py-4">
                <div class="w-20 h-20 bg-green-100 text-green-600 rounded-full flex items-center justify-center mx-auto text-3xl mb-6">
                    <i class="fa-solid fa-check"></i>
                </div>
                <h3 class="font-bold text-2xl text-gray-800"><?= $success ?></h3>
                <p class="text-gray-500 mt-2">Akun kamu sudah siap digunakan.</p>
                <a href="login.php" class="mt-8 block w-full bg-brand-primary hover:bg-brand-dark text-white py-4 rounded-2xl font-bold transition-all shadow-lg shadow-brand-primary/20 text-center">
                    MASUK SEKARANG <i class="fa-solid fa-arrow-right ml-2"></i>
                </a>
            </div>
        <?php endif; ?>

        <?php if (!$registered): ?>
            <form action="" method="POST" class="space-y-5">
                <div>
                    <label class="block mb-2 text-sm font-medium text-gray-700">Nama Lengkap <span class="text-red-500">*</span></label>
                    <input type="text" name="nama_lengkap" required class="w-full px-5 py-3.5 rounded-2xl border border-gray-300 focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all placeholder:text-gray-400" placeholder="Masukan Nama Lengkap">
                </div>

                <div>
                    <label class="block mb-2 text-sm font-medium text-gray-700">Username <span class="text-red-500">*</span></label>
                    <input type="text" name="username" required class="w-full px-5 py-3.5 rounded-2xl border border-gray-300 focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all placeholder:text-gray-400" placeholder="Masukan Username">
                </div>

                <div>
                    <label class="block mb-2 text-sm font-medium text-gray-700">Password <span class="text-red-500">*</span></label>
                    <input type="password" name="password" required class="w-full px-5 py-3.5 rounded-2xl border border-gray-300 focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all placeholder:text-gray-400" placeholder="Masukan Password">
                </div>

                <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                    <div>
                        <label class="block mb-2 text-sm font-medium text-gray-700">Nama Usaha UMKM <span class="text-red-500">*</span></label>
                        <select name="id_umkm" required class="w-full px-5 py-3.5 rounded-2xl border border-gray-300 bg-white focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all text-sm font-medium text-gray-700 appearance-none">
                            <option value="">Pilih UMKM</option>
                            <?php 
                            if ($result_umkm && mysqli_num_rows($result_umkm) > 0) {
                                while ($umkm = mysqli_fetch_assoc($result_umkm)) {
                                    ?>
                                    <option value="<?php echo $umkm['id_umkm']; ?>">
                                        <?php echo htmlspecialchars($umkm['nama_usaha']); ?> - ID: <?php echo $umkm['id_umkm']; ?>
                                    </option>
                                    <?php
                                }
                            } else {
                                ?>
                                <option value="" disabled>Belum ada UMKM yang terdaftar</option>
                                <?php
                            }
                            ?>
                        </select>
                    </div>
                    <div>
                        <label class="block mb-2 text-sm font-medium text-gray-700">Role <span class="text-red-500">*</span></label>
                        <div class="relative">
                            <select name="role" required class="w-full px-5 py-3.5 rounded-2xl border border-gray-300 focus:outline-none focus:ring-2 focus:ring-brand-primary/20 focus:border-brand-primary transition-all appearance-none bg-white text-gray-700 cursor-pointer">
                                <option value="" disabled selected>Pilih Role</option>
                                <option value="Kasir">Kasir</option>
                                <option value="Admin">Admin</option>
                            </select>
                            <div class="absolute right-5 top-1/2 -translate-y-1/2 pointer-events-none text-gray-400">
                                <i class="fa-solid fa-chevron-down text-xs"></i>
                            </div>
                        </div>
                    </div>
                </div>

                <button type="submit" class="w-full bg-gradient-to-r from-brand-dark to-brand-primary text-white font-bold py-4 rounded-2xl shadow-lg shadow-brand-primary/20 hover:shadow-brand-primary/30 hover:-translate-y-0.5 transition-all active:scale-[0.98] mt-4 uppercase tracking-widest text-lg">
                    DAFTAR
                </button>
            </form>

            <div class="text-center mt-10">
                <span class="text-gray-500 text-sm">Sudah punya akun?</span> 
                <a href="login.php" class="text-brand-primary font-bold text-sm hover:underline ml-1">Masuk Sekarang</a>
            </div>
        <?php endif; ?>
    </div>
</body>
</html>