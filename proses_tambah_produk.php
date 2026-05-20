<?php
session_start();

if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit(); 
}

require_once 'koneksi.php'; 

if (isset($_POST['btn_simpan_produk'])) {
    
    $id_umkm      = intval($_POST['id_umkm']);
    $nama_produk  = mysqli_real_escape_string($conn, trim($_POST['nama_produk']));
    $kategori     = mysqli_real_escape_string($conn, trim($_POST['kategori'])); // Mengamankan input teks bebas
    $sisa_stok    = intval($_POST['sisa_stok']);
    $harga_beli   = floatval($_POST['harga_beli']);
    $harga_jual   = floatval($_POST['harga_jual']);

    if (empty($nama_produk) || empty($kategori) || $sisa_stok < 0 || $harga_beli < 0 || $harga_jual < 0) {
        header("Location: produk.php?status=gagal_input_kosong");
        exit();
    }

    $query = "INSERT INTO produk (id_umkm, nama_produk, kategori, harga_jual, harga_beli, sisa_stok) VALUES (?, ?, ?, ?, ?, ?)";
    
    if ($stmt = mysqli_prepare($conn, $query)) {
        
        mysqli_stmt_bind_param($stmt, "issddi", $id_umkm, $nama_produk, $kategori, $harga_jual, $harga_beli, $sisa_stok);
        
        if (mysqli_stmt_execute($stmt)) {
            header("Location: produk.php?status=sukses_tambah");
            exit();
        } else {
            echo "Gagal menyimpan data ke database: " . mysqli_stmt_error($stmt);
        }
        
        mysqli_stmt_close($stmt);
        
    } else {
        echo "Gagal menyiapkan struktur query: " . mysqli_error($conn);
    }
    
} else {
    header("Location: produk.php");
    exit();
}

mysqli_close($conn);
?>