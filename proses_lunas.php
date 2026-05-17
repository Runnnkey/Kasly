<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

if (isset($_GET['id']) && is_numeric($_GET['id'])) {
    $id_piutang = mysqli_real_escape_string($conn, $_GET['id']);

    mysqli_begin_transaction($conn);

    try {
        $query_piutang = "UPDATE piutang SET sisa_tagihan = 0, status = 'Lunas' WHERE id_piutang = '$id_piutang'";
        mysqli_query($conn, $query_piutang);

        $query_get_jual = "SELECT id_penjualan FROM piutang WHERE id_piutang = '$id_piutang'";
        $res_jual = mysqli_query($conn, $query_get_jual);
        
        if ($row_jual = mysqli_fetch_assoc($res_jual)) {
            $id_penjualan = $row_jual['id_penjualan'];
            
            $query_penjualan = "UPDATE penjualan SET status_bayar = 'Lunas' WHERE id_penjualan = '$id_penjualan'";
            mysqli_query($conn, $query_penjualan);
        }

        mysqli_commit($conn);
        
    } catch (Exception $e) {
        mysqli_rollback($conn);
    }
}

header("Location: utangPiutang.php");
exit();