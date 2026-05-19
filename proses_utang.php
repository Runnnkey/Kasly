<?php
session_start();

// 1. Validasi Sesi Pengguna
if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit(); 
}

require_once 'koneksi.php'; 

if (isset($_POST['btn_simpan_utang'])) {
    
    // 2. Tangkap Data dari Form HTML
    $id_pembelian  = intval($_POST['id_pembelian']);
    $id_supplier   = intval($_POST['id_supplier']);
    $nominal_bayar = floatval($_POST['nominal_bayar']); // nominal yang dibayarkan kali ini
    $jatuh_tempo   = $_POST['jatuh_tempo'];
    $status        = $_POST['status']; // 'Belum Lunas' atau 'Lunas'

    // Validasi dasar
    if (empty($id_pembelian) || empty($id_supplier) || empty($jatuh_tempo) || empty($status) || $nominal_bayar <= 0) {
        header("Location: utangPiutang.php?status=gagal_input_kosong");
        exit();
    }

    // Jalankan Database Transaction agar sinkronisasi aman
    mysqli_begin_transaction($conn);

    try {
        // 3. QUERY UPDATE: Kurangi sisa_utang secara langsung di tabel utang
        $query_update_utang = "UPDATE utang SET 
                                    sisa_utang = sisa_utang - ?, 
                                    jatuh_tempo = ?, 
                                    status = ? 
                               WHERE id_pembelian = ?";
        
        $stmt = mysqli_prepare($conn, $query_update_utang);
        mysqli_stmt_bind_param($stmt, "dssi", $nominal_bayar, $jatuh_tempo, $status, $id_pembelian);
        mysqli_stmt_execute($stmt);
        mysqli_stmt_close($stmt);

        // 4. SINKRONISASI: Jika sudah lunas, ubah status_bayar di tabel master pembelian menjadi Lunas
        if ($status === 'Lunas') {
            $query_update_pembelian = "UPDATE pembelian SET status_bayar = 'Lunas' WHERE id_pembelian = ?";
            $stmt_pb = mysqli_prepare($conn, $query_update_pembelian);
            mysqli_stmt_bind_param($stmt_pb, "i", $id_pembelian);
            mysqli_stmt_execute($stmt_pb);
            mysqli_stmt_close($stmt_pb);
        }

        mysqli_commit($conn);
        header("Location: utangPiutang.php?status=sukses_update_utang");
        exit();

    } catch (Exception $e) {
        mysqli_rollback($conn);
        echo "Gagal memperbarui data utang ke supplier: " . $e->getMessage();
    }

} else {
    header("Location: utangPiutang.php");
    exit();
}

mysqli_close($conn);
?>