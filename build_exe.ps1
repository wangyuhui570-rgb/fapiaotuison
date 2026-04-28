$ErrorActionPreference = "Stop"
[Console]::InputEncoding = [System.Text.UTF8Encoding]::new($false)
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [Console]::OutputEncoding
chcp 65001 > $null

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
$PyInstallerExe = Join-Path $ProjectRoot ".venv\Scripts\pyinstaller.exe"
$IconScript = Join-Path $ProjectRoot "generate_icons.py"
$AppIcon = Join-Path $ProjectRoot "assets\app_icon.ico"

if (-not (Test-Path $PythonExe)) {
    throw "未找到虚拟环境 Python：$PythonExe"
}

if (-not (Test-Path $PyInstallerExe)) {
    throw "未找到 PyInstaller：$PyInstallerExe"
}

Set-Location $ProjectRoot

if (-not (Test-Path $IconScript)) {
    throw "未找到图标生成脚本：$IconScript"
}

& $PythonExe $IconScript

if (-not (Test-Path $AppIcon)) {
    throw "未找到应用图标：$AppIcon"
}

& $PyInstallerExe `
  --noconfirm `
  --clean `
  --windowed `
  --name "InvoiceDownloader" `
  --icon "$AppIcon" `
  --collect-binaries pyzbar `
  --collect-data pyzbar `
  --hidden-import pyzbar.pyzbar `
  --hidden-import PySide6.QtCore `
  --hidden-import PySide6.QtGui `
  --hidden-import PySide6.QtWidgets `
  --add-data "assets;assets" `
  --add-data "USER_GUIDE.txt;." `
  invoice_desktop.py

$DistDir = Join-Path $ProjectRoot "dist\InvoiceDownloader"
Copy-Item -Force (Join-Path $ProjectRoot "USER_GUIDE.txt") (Join-Path $DistDir "USER_GUIDE.txt")

Write-Host ""
Write-Host "打包完成。输出目录：$ProjectRoot\dist\InvoiceDownloader"
