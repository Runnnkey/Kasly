<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

$user_id = $_SESSION['user_id'];

$queryUser = "SELECT id_umkm FROM user WHERE id_user = '$user_id'";
$resultUser = mysqli_query($conn, $queryUser);
$rowUser = mysqli_fetch_assoc($resultUser);
$id_umkm = $rowUser['id_umkm'];

$id_produk = isset($_GET['id']) ? (int)$_GET['id'] : 0;

if ($id_produk > 0) {
    // Cek apakah produk milik UMKM ini
    $queryCek = "SELECT id_produk FROM produk WHERE id_produk = $id_produk AND id_umkm = $id_umkm";
    $resultCek = mysqli_query($conn, $queryCek);

    if (mysqli_num_rows($resultCek) > 0) {
        $queryHapus = "DELETE FROM produk WHERE id_produk = $id_produk";
        if (mysqli_query($conn, $queryHapus)) {
            // Redirect dengan status sukses
            header("Location: produk.php?status=hapus_sukses");
            exit();
        } else {
            // Redirect dengan status gagal
            header("Location: produk.php?status=hapus_gagal");
            exit();
        }
    } else {
        header("Location: produk.php");
        exit();
    }
} else {
    header("Location: produk.php");
    exit();
}
?>