<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

if ($_SERVER['REQUEST_METHOD'] !== 'POST') {
    header("Location: transaksi.php");
    exit();
}

$id_user = $_POST['id_user'] ?? 0;
$id_umkm = $_POST['id_umkm'] ?? 0;
$tanggal_pembelian = $_POST['tanggal_pembelian'] ?? date('Y-m-d');
$metode_pembayaran = $_POST['metode_pembayaran_pembelian'] ?? 'Tunai';
$jatuh_tempo_utang = $_POST['jatuh_tempo_utang'] ?? null;
$total_biaya = $_POST['total_biaya'] ?? 0;
$detail_pembelian = json_decode($_POST['detail_pembelian'], true);
$id_supplier = $_POST['id_supplier'] ?? 0;

if ($total_biaya <= 0 || empty($detail_pembelian)) {
    $_SESSION['error'] = "Tidak ada produk yang dibeli!";
    header("Location: transaksi.php");
    exit();
}

if ($id_supplier == 0) {
    $_SESSION['error'] = "Pilih supplier terlebih dahulu!";
    header("Location: transaksi.php");
    exit();
}

mysqli_begin_transaction($conn);

try {
    $status_bayar = ($metode_pembayaran == 'Tunai') ? 'Lunas' : 'Belum Lunas';
    
    // 1. INSERT ke pembelian
    $query = "INSERT INTO pembelian (id_supplier, total_biaya, tanggal, status_bayar, id_umkm) 
              VALUES (?, ?, ?, ?, ?)";
    $stmt = mysqli_prepare($conn, $query);
    mysqli_stmt_bind_param($stmt, "iissi", $id_supplier, $total_biaya, $tanggal_pembelian, $status_bayar, $id_umkm);
    mysqli_stmt_execute($stmt);
    $id_pembelian = mysqli_insert_id($conn);
    mysqli_stmt_close($stmt);
    
    // 2. UPDATE stok dan INSERT stok_masuk
    foreach ($detail_pembelian as $item) {
        $id_produk = $item['id_produk'];
        $kuantitas = $item['qty'];
        $harga_beli = $item['harga_beli'];
        
        $query = "UPDATE produk SET sisa_stok = sisa_stok + ?, harga_beli = ? WHERE id_produk = ?";
        $stmt = mysqli_prepare($conn, $query);
        mysqli_stmt_bind_param($stmt, "iii", $kuantitas, $harga_beli, $id_produk);
        mysqli_stmt_execute($stmt);
        mysqli_stmt_close($stmt);
        
        $query = "INSERT INTO stok_masuk (id_produk, jumlah_masuk, tanggal_masuk) VALUES (?, ?, ?)";
        $stmt = mysqli_prepare($conn, $query);
        mysqli_stmt_bind_param($stmt, "iis", $id_produk, $kuantitas, $tanggal_pembelian);
        mysqli_stmt_execute($stmt);
        mysqli_stmt_close($stmt);
    }
    
    // 3. Jika Kredit, INSERT ke utang
    if ($metode_pembayaran == 'Kredit' && !empty($jatuh_tempo_utang)) {
        $query = "INSERT INTO utang (id_pembelian, id_supplier, total_utang, sisa_utang, jatuh_tempo, status) 
                  VALUES (?, ?, ?, ?, ?, 'Belum Lunas')";
        $stmt = mysqli_prepare($conn, $query);
        mysqli_stmt_bind_param($stmt, "iiiis", $id_pembelian, $id_supplier, $total_biaya, $total_biaya, $jatuh_tempo_utang);
        mysqli_stmt_execute($stmt);
        mysqli_stmt_close($stmt);
    }
    
    mysqli_commit($conn);
    
    $_SESSION['success'] = "Transaksi pembelian berhasil disimpan!";
    header("Location: transaksi.php");
    exit();
    
} catch (Exception $e) {
    mysqli_rollback($conn);
    $_SESSION['error'] = "Gagal: " . $e->getMessage();
    header("Location: transaksi.php");
    exit();
}
?>