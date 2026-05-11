<?php
session_start(); // Wajib dipanggil untuk mengakses sesi yang ada

// Menghapus semua variabel session
$_SESSION = array();

// Jika ingin benar-benar menghapus sesi secara total (termasuk cookie sesi)
if (ini_get("session.use_cookies")) {
    $params = session_get_cookie_params();
    setcookie(session_name(), '', time() - 42000,
        $params["path"], $params["domain"],
        $params["secure"], $params["httponly"]
    );
}

// Menghancurkan session
session_destroy();

// Mengarahkan kembali ke halaman login
header("Location: login.php");
exit();
?>