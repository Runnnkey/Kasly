<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

if ($_SERVER['REQUEST_METHOD'] === 'POST') {
    $nama_supplier = trim(mysqli_real_escape_string($conn, $_POST['nama_supplier'] ?? ''));
    $kontak        = trim(mysqli_real_escape_string($conn, $_POST['kontak'] ?? ''));

    if (!empty($nama_supplier)) {
        $query = "INSERT INTO supplier (nama_supplier, kontak) VALUES ('$nama_supplier', '$kontak')";
        mysqli_query($conn, $query);
    }
}

header("Location: transaksi.php?buka=pembelian");
exit();