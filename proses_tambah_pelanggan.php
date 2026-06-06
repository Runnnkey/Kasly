<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $nama_pelanggan = trim(mysqli_real_escape_string($conn, $_POST['nama_pelanggan'] ?? ''));

    if (!empty($nama_pelanggan)) {
        $query = "INSERT INTO pelanggan (nama_pelanggan) VALUES ('$nama_pelanggan')";
        mysqli_query($conn, $query);
    }
}

header("Location: transaksi.php?buka=penjualan");
exit();