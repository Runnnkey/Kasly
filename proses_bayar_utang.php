<?php
session_start();

// 1. Validasi Sesi Pengguna
if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit(); 
}

require_once 'koneksi.php'; 

// 2. Ambil ID Utang dari URL
if (isset($_GET['id'])) {
    $id_utang = intval($_GET['id']);

    // Mulai Database Transaction (Menjamin sinkronisasi kedua tabel aman)
    mysqli_begin_transaction($conn);

    try {
        // A. Cari id_pembelian terkait dari data utang ini terlebih dahulu
        $query_get_id = "SELECT id_pembelian FROM utang WHERE id_utang = ?";
        $stmt_get = mysqli_prepare($conn, $query_get_id);
        mysqli_stmt_bind_param($stmt_get, "i", $id_utang);
        mysqli_stmt_execute($stmt_get);
        $result_get = mysqli_stmt_get_result($stmt_get);
        $data_utang = mysqli_fetch_assoc($result_get);
        mysqli_stmt_close($stmt_get);

        if ($data_utang) {
            $id_pembelian = $data_utang['id_pembelian'];

            // B. UPDATE TABEL UTANG: Set sisa_utang menjadi 0 dan status menjadi Lunas
            $query_update_utang = "UPDATE utang SET sisa_utang = 0, status = 'Lunas' WHERE id_utang = ?";
            $stmt_u = mysqli_prepare($conn, $query_update_utang);
            mysqli_stmt_bind_param($stmt_u, "i", $id_utang);
            mysqli_stmt_execute($stmt_u);
            mysqli_stmt_close($stmt_u);

            // C. UPDATE TABEL PEMBELIAN: Ubah status_bayar menjadi Lunas (Ini kunci penyaringan dropdown!)
            // Catatan: Pastikan nama kolom status bayar di tabel pembelian Anda adalah 'status_bayar'
            $query_update_pembelian = "UPDATE pembelian SET status_bayar = 'Lunas' WHERE id_pembelian = ?";
            $stmt_pb = mysqli_prepare($conn, $query_update_pembelian);
            mysqli_stmt_bind_param($stmt_pb, "i", $id_pembelian);
            mysqli_stmt_execute($stmt_pb);
            mysqli_stmt_close($stmt_pb);

            // Jika kedua tabel berhasil di-update tanpa error, simpan permanen
            mysqli_commit($conn);
            
            header("Location: utangPiutang.php?status=utang_lunas");
            exit();
        } else {
            throw new Exception("Data utang tidak ditemukan.");
        }

    } catch (Exception $e) {
        // Jika salah satu proses gagal, batalkan semua agar data tidak selisih
        mysqli_rollback($conn);
        header("Location: utangPiutang.php?status=gagal_lunas");
        exit();
    }

} else {
    header("Location: utangPiutang.php");
    exit();
}

mysqli_close($conn);
?>