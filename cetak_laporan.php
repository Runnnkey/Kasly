<?php
session_start();
if (!isset($_SESSION['user_id']) || empty($_SESSION['user_id'])) {
    header("Location: login.php");
    exit();
}

require_once 'koneksi.php';
require_once 'vendor/autoload.php'; // Load Dompdf

use Dompdf\Dompdf;
use Dompdf\Options;

$user_id = $_SESSION['user_id'];

// Ambil data user & id_umkm (Sama persis seperti di laporan.php)
$queryUser = "SELECT id_umkm, nama_lengkap FROM user WHERE id_user = '$user_id'";
$resultUser = mysqli_query($conn, $queryUser);
$rowUser = mysqli_fetch_assoc($resultUser);
$id_umkm = $rowUser['id_umkm'];

// 1. Ambil data keuangan esensial
$totalOmzet = mysqli_fetch_assoc(mysqli_query($conn, "SELECT SUM(total_harga) as total FROM penjualan WHERE id_umkm = '$id_umkm' AND status_bayar = 'Lunas'"))['total'] ?? 0;
$totalBeban = mysqli_fetch_assoc(mysqli_query($conn, "SELECT SUM(total_biaya) as total FROM pembelian WHERE id_umkm = '$id_umkm'"))['total'] ?? 0;
$labaBersih = $totalOmzet - $totalBeban;

// 2. Ambil Data 5 Produk Terlaris
$queryTop5 = "SELECT p.nama_produk, SUM(pd.kuantitas) as total_terjual
              FROM penjualan_detail pd
              JOIN produk p ON pd.id_produk = p.id_produk
              JOIN penjualan pj ON pd.id_penjualan = pj.id_penjualan
              WHERE pj.id_umkm = '$id_umkm' AND pj.status_bayar = 'Lunas'
              GROUP BY p.id_produk, p.nama_produk
              ORDER BY total_terjual DESC LIMIT 5";
$resTop5 = mysqli_query($conn, $queryTop5);

// Struktur HTML khusus yang akan diubah menjadi PDF
$html = '
<!DOCTYPE html>
<html>
<head>
    <style>
        body { font-family: sans-serif; color: #334155; margin: 20px; }
        .header { text-align: center; margin-bottom: 30px; border-bottom: 2px solid #e2e8f0; padding-bottom: 10px; }
        .title { font-size: 24px; font-weight: bold; color: #1e1b4b; }
        .subtitle { font-size: 12px; color: #64748b; }
        .grid { width: 100%; margin-bottom: 30px; }
        .card { width: 30%; display: inline-block; box-sizing: border-box; background: #f8fafc; padding: 15px; border-radius: 10px; border: 1px solid #e2e8f0; margin-right: 2%; }
        .card-laba { background: #e0e7ff; border-color: #c7d2fe; }
        .card p { margin: 0; font-size: 10px; text-transform: uppercase; color: #64748b; font-weight: bold; }
        .card h3 { margin: 5px 0 0 0; font-size: 16px; color: #0f172a; }
        table { width: 100%; border-collapse: collapse; margin-top: 15px; }
        th, td { padding: 10px; text-align: left; font-size: 12px; border-bottom: 1px solid #e2e8f0; }
        th { background-color: #f1f5f9; color: #475569; font-weight: bold; }
    </style>
</head>
<body>

    <div class="header">
        <div class="title">KASLY - LAPORAN KEUANGAN UMKM</div>
        <div class="subtitle">Dicetak pada: ' . date('d F Y H:i') . ' | Oleh: ' . htmlspecialchars($rowUser['nama_lengkap']) . '</div>
    </div>

    <div class="grid">
        <div class="card">
            <p>Total Omzet</p>
            <h3>Rp ' . number_format($totalOmzet, 0, ',', '.') . '</h3>
        </div>
        <div class="card">
            <p>Total Beban</p>
            <h3>Rp ' . number_format($totalBeban, 0, ',', '.') . '</h3>
        </div>
        <div class="card card-laba">
            <p>Laba Bersih</p>
            <h3>Rp ' . number_format($labaBersih, 0, ',', '.') . '</h3>
        </div>
    </div>

    <h4>🏆 5 Produk Terlaris</h4>
    <table>
        <thead>
            <tr>
                <th style="width: 10%">No</th>
                <th style="width: 60%">Nama Produk</th>
                <th style="width: 30%; text-align: right;">Total Terjual</th>
            </tr>
        </thead>
        <tbody>';
        
        $rank = 1;
        while ($produk = mysqli_fetch_assoc($resTop5)) {
            $html .= '
            <tr>
                <td>' . sprintf("%02d", $rank) . '</td>
                <td>' . htmlspecialchars($produk['nama_produk']) . '</td>
                <td style="text-align: right;">' . $produk['total_terjual'] . ' pcs</td>
            </tr>';
            $rank++;
        }

$html .= '
        </tbody>
    </table>

</body>
</html>';

// Setup DOMPDF Options
$options = new Options();
$options->set('isHtml5ParserEnabled', true);
$options->set('isRemoteEnabled', true);

$dompdf = new Dompdf($options);
$dompdf->loadHtml($html);
$dompdf->setPaper('A4', 'portrait');
$dompdf->render();

// Keluarkan file ke browser untuk diunduh otomatis
$dompdf->stream("Laporan_Kasly_" . date('Ymd') . ".pdf", array("Attachment" => 1));
exit();
?>