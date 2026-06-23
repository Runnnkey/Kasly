<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

$user_id = $_SESSION['user_id'];

$queryUser = "SELECT role, id_umkm FROM user WHERE id_user = '$user_id'";
$resultUser = mysqli_query($conn, $queryUser);
$user = mysqli_fetch_assoc($resultUser);

if ($user['role'] !== 'Owner') {
    $_SESSION['error'] = "Akses ditolak! Hanya Owner yang bisa mengedit user.";
    header("Location: index.php");
    exit();
}

$id_umkm = $user['id_umkm'];
$id_edit = isset($_GET['id']) ? $_GET['id'] : 0;

// Ambil data user yang akan diedit
$queryEdit = "SELECT * FROM user WHERE id_user = '$id_edit' AND id_umkm = '$id_umkm' AND role != 'Owner'";
$resultEdit = mysqli_query($conn, $queryEdit);

if (mysqli_num_rows($resultEdit) === 0) {
    header("Location: manage_user.php");
    exit();
}

$dataUser = mysqli_fetch_assoc($resultEdit);

// Proses update
if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $nama_lengkap = trim($_POST['nama_lengkap']);
    $username = trim($_POST['username']);
    $role = $_POST['role'];
    $password = $_POST['password'];

    // Cek username (kecuali dirinya sendiri)
    $queryCek = "SELECT id_user FROM user WHERE username = '$username' AND id_user != '$id_edit'";
    $resultCek = mysqli_query($conn, $queryCek);
    if (mysqli_num_rows($resultCek) > 0) {
        $_SESSION['error'] = "Username sudah digunakan oleh user lain!";
        header("Location: edit_user.php?id=$id_edit");
        exit();
    }

   
    $queryUpdate = "UPDATE user SET 
                        nama_lengkap = '$nama_lengkap',
                        username = '$username'
                        WHERE id_user = '$id_edit'";
    

    if (mysqli_query($conn, $queryUpdate)) {
        header("Location: manage_user.php?status=edit_sukses");
        exit();
    } else {
        $error = "Gagal mengupdate user: " . mysqli_error($conn);
    }
}
?>

<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Edit User - Kasly</title>
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
                <?php echo strtoupper(substr($user['nama_lengkap'] ?? 'O', 0, 1)); ?>
            </div>
        </div>
    </nav>

    <section class="space-y-8 p-4 md:p-6 max-w-2xl mx-auto">
        <div class="bg-white border border-slate-200 rounded-3xl p-6 shadow-sm">
            <div class="flex items-center gap-3 mb-6 border-b border-slate-100 pb-4">
                <div class="w-10 h-10 bg-indigo-50 text-indigo-600 rounded-xl flex items-center justify-center">
                    <i class="fa-solid fa-pen text-base"></i>
                </div>
                <div>
                    <h3 class="text-base font-black text-slate-800 tracking-tight">Edit User</h3>
                    <p class="text-xs text-slate-400 font-medium">Ubah data user yang sudah ada.</p>
                </div>
            </div>

            <?php if (isset($error)): ?>
                <div class="p-3 mb-4 bg-rose-50 border border-rose-200 text-rose-700 rounded-xl text-xs"><?php echo $error; ?></div>
            <?php endif; ?>
            <?php if (isset($_SESSION['error'])): ?>
                <div class="p-3 mb-4 bg-rose-50 border border-rose-200 text-rose-700 rounded-xl text-xs"><?php echo $_SESSION['error']; unset($_SESSION['error']); ?></div>
            <?php endif; ?>

            <form action="" method="POST" class="space-y-4">
                <div class="space-y-1.5">
                    <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Nama Lengkap <span class="text-red-500">*</span></label>
                    <input type="text" name="nama_lengkap" required value="<?php echo htmlspecialchars($dataUser['nama_lengkap']); ?>"
                        class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-700">
                </div>

                <div class="space-y-1.5">
                    <label class="text-[10px] font-bold text-slate-400 uppercase tracking-wider">Username <span class="text-red-500">*</span></label>
                    <input type="text" name="username" required value="<?php echo htmlspecialchars($dataUser['username']); ?>"
                        class="w-full px-4 py-3 bg-slate-50 border border-slate-200 rounded-xl focus:ring-2 focus:ring-indigo-500 outline-none text-sm font-medium text-slate-700">
                </div>


                <div class="flex justify-end gap-3 pt-2">
                    <a href="manage_user.php" class="px-6 py-2.5 bg-slate-200 text-slate-700 rounded-xl text-xs font-bold hover:bg-slate-300 transition-all">Batal</a>
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