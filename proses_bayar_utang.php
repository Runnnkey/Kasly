<?php
session_start();

// 1. Validasi akses pengguna yang sedang login
if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';

// Pastikan parameter ID utang dikirimkan dan berupa angka valid
if (isset($_GET['id']) && is_numeric($_GET['id'])) {
    $id_utang = mysqli_real_escape_string($conn, $_GET['id']);

    // Mulai database transaction untuk menjaga konsistensi data di beberapa tabel
    mysqli_begin_transaction($conn);

    try {
        // A. Ambil detail utang sebelum diubah (untuk mendapatkan nominal sisa_utang & id_pembelian)
        $query_get_utang = "SELECT id_pembelian, id_supplier, sisa_utang FROM utang WHERE id_utang = '$id_utang'";
        $res_utang = mysqli_query($conn, $query_get_utang);
        
        if ($utang_data = mysqli_fetch_assoc($res_utang)) {
            $id_pembelian = $utang_data['id_pembelian'];
            $id_supplier  = $utang_data['id_supplier'];
            $jumlah_bayar = $utang_data['sisa_utang']; // Bayar lunas seluruh sisa utang

            if ($jumlah_bayar > 0) {
                // B. Catat riwayat pembayaran ke dalam tabel pembayaran_utang
                $query_log = "INSERT INTO pembayaran_utang (id_utang, tanggal_pembayaran, jumlah_bayar, metode_pembayaran, keterangan) 
                              VALUES ('$id_utang', NOW(), '$jumlah_bayar', 'Tunai', 'Pelunasan Instan via Dashboard')";
                mysqli_query($conn, $query_log);
            }

            // C. Update status utang menjadi Lunas dan kosongkan sisa_utang
            $query_update_utang = "UPDATE utang SET sisa_utang = 0, status = 'Lunas' WHERE id_utang = '$id_utang'";
            mysqli_query($conn, $query_update_utang);
        }

        // Komit semua transaksi jika tidak ada error
        mysqli_commit($conn);
        
    } catch (Exception $e) {
        // Jika ada query yang gagal, batalkan semua perubahan data
        mysqli_rollback($conn);
        die("Gagal memproses pembayaran utang: " . $e->getMessage());
    }
}

// Kembalikan pengguna ke halaman utama secara instan setelah data ter-update
header("Location: utangPiutang.php");
exit();