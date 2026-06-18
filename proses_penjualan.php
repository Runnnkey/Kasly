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
$tanggal_transaksi = $_POST['tanggal_transaksi'] ?? date('Y-m-d');
$metode_pembayaran = $_POST['metode_pembayaran'] ?? 'Tunai';
$jatuh_tempo = $_POST['jatuh_tempo'] ?? null;
$total_harga = $_POST['total_harga'] ?? 0;
$detail_transaksi = json_decode($_POST['detail_transaksi'], true);
$id_pelanggan = $_POST['id_pelanggan'] ?? 0;

if ($total_harga <= 0 || empty($detail_transaksi)) {
    header("Location: transaksi.php?status=penjualan_gagal");
    exit();
}

if ($id_pelanggan == 0) {
    header("Location: transaksi.php?status=penjualan_gagal");
    exit();
}

mysqli_begin_transaction($conn);

try {
    $status_bayar = ($metode_pembayaran == 'Tunai') ? 'Lunas' : 'Belum Lunas';
    
    // 1. INSERT ke penjualan
    $query = "INSERT INTO penjualan (id_user, tanggal_transaksi, total_harga, metode_pembayaran, status_bayar, id_umkm) 
              VALUES (?, ?, ?, ?, ?, ?)";
    $stmt = mysqli_prepare($conn, $query);
    mysqli_stmt_bind_param($stmt, "isissi", $id_user, $tanggal_transaksi, $total_harga, $metode_pembayaran, $status_bayar, $id_umkm);
    mysqli_stmt_execute($stmt);
    $id_penjualan = mysqli_insert_id($conn);
    mysqli_stmt_close($stmt);
    
    // 2. INSERT ke penjualan_detail dan UPDATE stok
    foreach ($detail_transaksi as $item) {
        $id_produk = $item['id_produk'];
        $kuantitas = $item['qty'];
        $subtotal = $item['qty'] * $item['harga'];
        
        $queryCek = "SELECT sisa_stok, nama_produk FROM produk WHERE id_produk = ?";
        $stmtCek = mysqli_prepare($conn, $queryCek);
        mysqli_stmt_bind_param($stmtCek, "i", $id_produk);
        mysqli_stmt_execute($stmtCek);
        $resCek = mysqli_stmt_get_result($stmtCek);
        $dataProduk = mysqli_fetch_assoc($resCek);
        mysqli_stmt_close($stmtCek);

        if (!$dataProduk || $dataProduk['sisa_stok'] < $kuantitas) {
            throw new Exception("Stok tidak mencukupi");
        }
        // -----------------------------------------------------

        $query = "INSERT INTO penjualan_detail (id_penjualan, id_produk, kuantitas, subtotal) VALUES (?, ?, ?, ?)";
        $stmt = mysqli_prepare($conn, $query);
        mysqli_stmt_bind_param($stmt, "iiii", $id_penjualan, $id_produk, $kuantitas, $subtotal);
        mysqli_stmt_execute($stmt);
        mysqli_stmt_close($stmt);
        
        $query = "UPDATE produk SET sisa_stok = sisa_stok - ? WHERE id_produk = ?";
        $stmt = mysqli_prepare($conn, $query);
        mysqli_stmt_bind_param($stmt, "ii", $kuantitas, $id_produk);
        mysqli_stmt_execute($stmt);
        mysqli_stmt_close($stmt);
    }
    
    // 3. Jika Kredit, INSERT ke piutang
    if ($metode_pembayaran == 'Kredit' && !empty($jatuh_tempo)) {
        $query = "INSERT INTO piutang (id_penjualan, id_pelanggan, sisa_tagihan, jatuh_tempo, status) 
                  VALUES (?, ?, ?, ?, 'Belum Lunas')";
        $stmt = mysqli_prepare($conn, $query);
        mysqli_stmt_bind_param($stmt, "iiis", $id_penjualan, $id_pelanggan, $total_harga, $jatuh_tempo);
        mysqli_stmt_execute($stmt);
        mysqli_stmt_close($stmt);
    }
    
    mysqli_commit($conn);
    header("Location: transaksi.php?status=penjualan_sukses");
    exit();
    
} catch (Exception $e) {
    mysqli_rollback($conn);
    header("Location: transaksi.php?status=penjualan_gagal");
    exit();
}
?>