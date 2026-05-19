<?php
session_start();

// 1. Validasi Sesi Pengguna
if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit(); 
}

require_once 'koneksi.php'; 

if (isset($_POST['btn_simpan_piutang'])) {
    
    $id_penjualan  = intval($_POST['id_penjualan']);
    $id_pelanggan  = intval($_POST['id_pelanggan']);
    $nominal_bayar = floatval($_POST['nominal_bayar']); 
    $jatuh_tempo   = $_POST['jatuh_tempo'];
    $status        = $_POST['status']; 
    if (empty($id_penjualan) || empty($id_pelanggan) || empty($jatuh_tempo) || empty($status) || $nominal_bayar <= 0) {
        header("Location: utangPiutang.php?status=gagal_input_kosong");
        exit();
    }

    mysqli_begin_transaction($conn);

    try {
        $query_update_piutang = "UPDATE piutang SET 
                                    sisa_tagihan = sisa_tagihan - ?, 
                                    jatuh_tempo = ?, 
                                    status = ? 
                                 WHERE id_penjualan = ?";
        
        $stmt = mysqli_prepare($conn, $query_update_piutang);
        
        mysqli_stmt_bind_param($stmt, "dssi", $nominal_bayar, $jatuh_tempo, $status, $id_penjualan);
        
        mysqli_stmt_execute($stmt);
        mysqli_stmt_close($stmt);

        if ($status === 'Lunas') {
            $query_update_penjualan = "UPDATE penjualan SET status_bayar = 'Lunas' WHERE id_penjualan = ?";
            $stmt_pj = mysqli_prepare($conn, $query_update_penjualan);
            mysqli_stmt_bind_param($stmt_pj, "i", $id_penjualan);
            mysqli_stmt_execute($stmt_pj);
            mysqli_stmt_close($stmt_pj);
        }

        mysqli_commit($conn);
        header("Location: utangPiutang.php?status=sukses_update_piutang");
        exit();

    } catch (Exception $e) {
        mysqli_rollback($conn);
        echo "Gagal memperbarui data piutang: " . $e->getMessage();
    }

} else {
    header("Location: utangPiutang.php");
    exit();
}

mysqli_close($conn);
?>