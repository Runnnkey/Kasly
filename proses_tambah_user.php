<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

$user_id = $_SESSION['user_id'];

// Cek apakah user adalah Owner
$queryUser = "SELECT role, id_umkm FROM user WHERE id_user = '$user_id'";
$resultUser = mysqli_query($conn, $queryUser);
$user = mysqli_fetch_assoc($resultUser);

if ($user['role'] !== 'Owner') {
    $_SESSION['error'] = "Akses ditolak! Hanya Owner yang bisa menambah user.";
    header("Location: index.php");
    exit();
}

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    header("Location: manage_user.php");
    exit();
}

$id_umkm = $_POST['id_umkm'];
$nama_lengkap = trim($_POST['nama_lengkap']);
$username = trim($_POST['username']);
$password = $_POST['password'];
$konfirmasi = $_POST['konfirmasi_password'];
$role = $_POST['role'];

// Validasi
if (empty($nama_lengkap) || empty($username) || empty($password) || empty($role)) {
    header("Location: manage_user.php?status=error&msg=Semua field harus diisi!");
    exit();
}

if ($password !== $konfirmasi) {
    header("Location: manage_user.php?status=error&msg=Password tidak cocok!");
    exit();
}

if (strlen($password) < 6) {
    header("Location: manage_user.php?status=error&msg=Password minimal 6 karakter!");
    exit();
}

// Cek username sudah ada belum
$queryCek = "SELECT id_user FROM user WHERE username = '$username'";
$resultCek = mysqli_query($conn, $queryCek);
if (mysqli_num_rows($resultCek) > 0) {
    header("Location: manage_user.php?status=error&msg=Username sudah digunakan!");
    exit();
}

// Hash password
$hashedPassword = password_hash($password, PASSWORD_DEFAULT);

// Simpan user
$queryInsert = "INSERT INTO user (id_umkm, username, password, role, nama_lengkap) 
                VALUES ('$id_umkm', '$username', '$hashedPassword', '$role', '$nama_lengkap')";

if (mysqli_query($conn, $queryInsert)) {
    header("Location: manage_user.php?status=tambah_sukses");
    exit();
} else {
    header("Location: manage_user.php?status=error&msg=Gagal menambah user: " . mysqli_error($conn));
    exit();
}
?>