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
    $_SESSION['error'] = "Akses ditolak! Hanya Owner yang bisa menghapus user.";
    header("Location: index.php");
    exit();
}

$id_umkm = $user['id_umkm'];
$id_hapus = isset($_GET['id']) ? $_GET['id'] : 0;

// Cek user yang akan dihapus
$queryCek = "SELECT * FROM user WHERE id_user = '$id_hapus' AND id_umkm = '$id_umkm' AND role != 'Owner'";
$resultCek = mysqli_query($conn, $queryCek);

if (mysqli_num_rows($resultCek) === 0) {
    header("Location: manage_user.php");
    exit();
}

// Hapus user
$queryHapus = "DELETE FROM user WHERE id_user = '$id_hapus'";

if (mysqli_query($conn, $queryHapus)) {
    header("Location: manage_user.php?status=hapus_sukses");
    exit();
} else {
    header("Location: manage_user.php?status=error&msg=Gagal menghapus user: " . mysqli_error($conn));
    exit();
}
?>