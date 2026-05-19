<?php
session_start();

// 1. Validasi Sesi Pengguna
if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit(); 
}

require_once 'koneksi.php'; 

// 2. Ambil ID Piutang dari URL
if (isset($_GET['id'])) {
    $id_piutang = intval($_GET['id']);

    // Mulai Database Transaction (Menjaga agar kedua query wajib sukses bersamaan)
    mysqli_begin_transaction($conn);

    try {
        // A. Ambil id_penjualan terkait dari piutang ini terlebih dahulu
        $query_get_id = "SELECT id_penjualan FROM piutang WHERE id_piutang = ?";
        $stmt_get = mysqli_prepare($conn, $query_get_id);
        mysqli_stmt_bind_param($stmt_get, "i", $id_piutang);
        mysqli_stmt_execute($stmt_get);
        $result_get = mysqli_stmt_get_result($stmt_get);
        $data_piutang = mysqli_fetch_assoc($result_get);
        mysqli_stmt_close($stmt_get);

        if ($data_piutang) {
            $id_penjualan = $data_piutang['id_penjualan'];

            // B. UPDATE TABEL PIUTANG: Ubah sisa_tagihan menjadi 0 dan status menjadi Lunas
            $query_update_piutang = "UPDATE piutang SET sisa_tagihan = 0, status = 'Lunas' WHERE id_piutang = ?";
            $stmt_p = mysqli_prepare($conn, $query_update_piutang);
            mysqli_stmt_bind_param($stmt_p, "i", $id_piutang);
            mysqli_stmt_execute($stmt_p);
            mysqli_stmt_close($stmt_p);

            // C. UPDATE TABEL PENJUALAN: Ubah status_bayar menjadi Lunas (Ini yang membuat dropdown sinkron!)
            $query_update_penjualan = "UPDATE penjualan SET status_bayar = 'Lunas' WHERE id_penjualan = ?";
            $stmt_pj = mysqli_prepare($conn, $query_update_penjualan);
            mysqli_stmt_bind_param($stmt_pj, "i", $id_penjualan);
            mysqli_stmt_execute($stmt_pj);
            mysqli_stmt_close($stmt_pj);

            // Jika semua query sukses tanpa error, terapkan ke database
            mysqli_commit($conn);
            
            header("Location: utangPiutang.php?status=sukses_lunas");
            exit();
        } else {
            throw new Exception("Data piutang tidak ditemukan.");
        }

    } catch (Exception $e) {
        // Jika salah satu query gagal, batalkan semua perubahan agar data tidak korup
        mysqli_rollback($conn);
        header("Location: utangPiutang.php?status=gagal_proses");
        exit();
    }

} else {
    header("Location: utangPiutang.php");
    exit();
}

mysqli_close($conn);
?>