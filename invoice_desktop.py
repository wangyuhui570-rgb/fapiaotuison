import contextlib
import ctypes
import hashlib
import json
import os
import re
import shutil
import socket
import sys
import threading
import time
import traceback
import uuid

from PySide6.QtCore import QObject, QPoint, QThread, Qt, Signal, Slot, QTimer, QUrl, QSize, QDate, QDateTime
from PySide6.QtGui import QAction, QColor, QDesktopServices, QFont, QIcon, QPixmap
from PySide6.QtWidgets import (
    QApplication,
    QComboBox,
    QDateTimeEdit,
    QDoubleSpinBox,
    QFileDialog,
    QFrame,
    QGraphicsDropShadowEffect,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPlainTextEdit,
    QPushButton,
    QSizePolicy,
    QStyle,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

import main
import quick_send


def app_base_dir():
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.abspath(os.getcwd())


def resource_base_dir():
    if getattr(sys, "frozen", False):
        return getattr(sys, "_MEIPASS", os.path.dirname(sys.executable))
    return os.path.abspath(os.getcwd())


APP_DIR = app_base_dir()
RESOURCE_DIR = resource_base_dir()
DEFAULT_INPUT_DIR = os.path.join(APP_DIR, "qrcode_images")
DEFAULT_OUTPUT_DIR = os.path.join(APP_DIR, "invoice_pdfs")
GUIDE_PATH = os.path.join(APP_DIR, "USER_GUIDE.txt")
ERROR_LOG_PATH = os.path.join(APP_DIR, "desktop_error.log")
UI_STATE_PATH = os.path.join(APP_DIR, "ui_state.json")
ASSETS_DIR = os.path.join(RESOURCE_DIR, "assets")
ICONS_DIR = os.path.join(ASSETS_DIR, "icons")
APP_ICON_PATH = os.path.join(ASSETS_DIR, "app_icon.ico")
APP_ICON_PNG_PATH = os.path.join(ASSETS_DIR, "app_icon.png")

MIN_WINDOW_WIDTH = 660
MIN_WINDOW_HEIGHT = 620
LOG_OPEN_HEIGHT = 700
DEFAULT_WECOM_WEBHOOK_URL = ""
DEFAULT_WECOM_WEBHOOK_NOTE = ""


PROCESS_MODE_PUSH = "push"
PROCESS_MODE_LOCAL = "local"
SINGLE_INSTANCE_HOST = "127.0.0.1"


def normalize_process_mode(value):
    if value == PROCESS_MODE_PUSH:
        return PROCESS_MODE_PUSH
    return PROCESS_MODE_LOCAL


def single_instance_port():
    digest = hashlib.sha256(APP_DIR.encode("utf-8", errors="ignore")).digest()
    return 42000 + int.from_bytes(digest[:2], "big") % 10000


class SingleInstanceBridge(QObject):
    shutdown_requested = Signal()


class SingleInstanceCoordinator:
    def __init__(self, app):
        self.app = app
        self.bridge = SingleInstanceBridge()
        self.bridge.shutdown_requested.connect(self.app.quit)
        self.host = SINGLE_INSTANCE_HOST
        self.port = single_instance_port()
        self.server_socket = None
        self.server_thread = None
        self.stop_event = threading.Event()

    def _start_listener(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(5)
        server_socket.settimeout(0.5)
        self.server_socket = server_socket

        def serve():
            while not self.stop_event.is_set():
                try:
                    client_socket, _ = server_socket.accept()
                except socket.timeout:
                    continue
                except OSError:
                    break

                with client_socket:
                    try:
                        payload = client_socket.recv(128).decode("utf-8", errors="ignore").strip()
                    except OSError:
                        continue
                    if payload == "shutdown":
                        self.bridge.shutdown_requested.emit()

        self.server_thread = threading.Thread(target=serve, name="invoice-desktop-single-instance", daemon=True)
        self.server_thread.start()

    def _request_shutdown(self):
        try:
            with socket.create_connection((self.host, self.port), timeout=0.5) as client_socket:
                client_socket.sendall(b"shutdown")
            return True
        except OSError:
            return False

    def acquire(self, timeout_seconds=8):
        deadline = time.time() + timeout_seconds
        requested_shutdown = False

        while time.time() < deadline:
            try:
                self._start_listener()
                return
            except OSError:
                if not requested_shutdown:
                    self._request_shutdown()
                    requested_shutdown = True
                time.sleep(0.2)

        raise RuntimeError("无法接管已运行的窗口，请先关闭旧窗口后重试。")

    def stop(self):
        self.stop_event.set()
        if self.server_socket is not None:
            try:
                self.server_socket.close()
            except OSError:
                pass
            self.server_socket = None


def load_guide_text():
    if os.path.exists(GUIDE_PATH):
        with open(GUIDE_PATH, "r", encoding="utf-8") as file_obj:
            return file_obj.read()
    return (
        "使用步骤\n"
        "1. 选择二维码图片目录。\n"
        "2. 选择发票输出目录。\n"
        "3. 点击“开始下载”。\n"
        "4. 已处理发票会自动跳过，失败项目会写入 failed.txt。\n"
        "5. 如需清理重复 PDF，请在“更多”菜单中操作。\n"
    )


def load_icon(name, fallback=None):
    path = os.path.join(ICONS_DIR, f"{name}.png")
    if os.path.exists(path):
        return QIcon(path)
    if fallback is not None:
        return fallback
    return QIcon()


def load_app_icon():
    if os.path.exists(APP_ICON_PATH):
        return QIcon(APP_ICON_PATH)
    if os.path.exists(APP_ICON_PNG_PATH):
        return QIcon(APP_ICON_PNG_PATH)
    fallback_path = os.path.join(ICONS_DIR, "download.png")
    if os.path.exists(fallback_path):
        return QIcon(fallback_path)
    return QIcon()


def set_windows_app_id():
    if sys.platform != "win32":
        return
    try:
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("Mayn.InvoiceDownloader")
    except Exception:
        return


def is_supported_image_file(file_path):
    return os.path.isfile(file_path) and file_path.lower().endswith((".png", ".jpg", ".jpeg", ".bmp", ".webp"))


def copy_images_to_directory(file_paths, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    copied_paths = []

    for source_path in file_paths:
        if not is_supported_image_file(source_path):
            continue

        file_name = os.path.basename(source_path)
        stem, ext = os.path.splitext(file_name)
        destination_path = os.path.join(target_dir, file_name)

        if os.path.exists(destination_path):
            destination_path = os.path.join(target_dir, f"{stem}_{uuid.uuid4().hex[:8]}{ext.lower()}")

        shutil.copy2(source_path, destination_path)
        copied_paths.append(destination_path)

    return copied_paths


def apply_soft_shadow(widget, blur=30, y_offset=8, alpha=28):
    shadow = QGraphicsDropShadowEffect(widget)
    shadow.setBlurRadius(blur)
    shadow.setOffset(0, y_offset)
    shadow.setColor(QColor(15, 23, 42, alpha))
    widget.setGraphicsEffect(shadow)


class SignalWriter(QObject):
    wrote = Signal(str)

    def __init__(self):
        super().__init__()
        self._buffer = ""

    def write(self, text):
        if not text:
            return
        self._buffer += text
        if "\n" not in self._buffer:
            return
        lines = self._buffer.splitlines(keepends=True)
        if lines and not lines[-1].endswith(("\n", "\r")):
            self._buffer = lines.pop()
        else:
            self._buffer = ""
        if lines:
            self.wrote.emit("".join(lines))

    def flush(self):
        if self._buffer:
            self.wrote.emit(self._buffer)
            self._buffer = ""
        return None


class Worker(QObject):
    log = Signal(str)
    finished = Signal(bool, str)

    def __init__(self, action_name, task):
        super().__init__()
        self.action_name = action_name
        self.task = task

    @Slot()
    def run(self):
        writer = SignalWriter()
        writer.wrote.connect(self.log.emit)
        try:
            with contextlib.redirect_stdout(writer), contextlib.redirect_stderr(writer):
                self.task()
                writer.flush()
            self.finished.emit(True, f"{self.action_name}已完成。")
        except Exception:
            self.log.emit(traceback.format_exc())
            self.finished.emit(False, f"{self.action_name}执行失败。")


class TaskWorker(QObject):
    log = Signal(str)
    finished = Signal(bool, str, object)

    def __init__(self, action_name, task):
        super().__init__()
        self.action_name = action_name
        self.task = task

    @Slot()
    def run(self):
        writer = SignalWriter()
        writer.wrote.connect(self.log.emit)
        try:
            with contextlib.redirect_stdout(writer), contextlib.redirect_stderr(writer):
                result = self.task()
                writer.flush()
            self.finished.emit(True, f"{self.action_name}已完成。", result)
        except Exception:
            self.log.emit(traceback.format_exc())
            self.finished.emit(False, f"{self.action_name}执行失败。", None)


class ClickableLabel(QLabel):
    clicked = Signal()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.clicked.emit()
            event.accept()
            return
        super().mousePressEvent(event)


class PathField(QFrame):
    def __init__(self, label_text, line_edit, browse_handler, clear_handler=None, label_click_handler=None):
        super().__init__()
        self.setObjectName("PathField")
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setMinimumHeight(76)
        self.drag_hint_button = None

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(6)

        header_row = QHBoxLayout()
        header_row.setContentsMargins(0, 0, 0, 0)
        header_row.setSpacing(8)
        label = ClickableLabel(label_text)
        label.setObjectName("FieldLabel")
        if label_click_handler is not None:
            label.setCursor(Qt.PointingHandCursor)
            label.clicked.connect(label_click_handler)
        header_row.addWidget(label)
        self.label = label

        header_row.addStretch(1)

        if label_click_handler is not None:
            drag_hint_button = QPushButton("拖入")
            drag_hint_button.setObjectName("FieldHintButton")
            drag_hint_button.setProperty("role", "ghost")
            drag_hint_button.clicked.connect(label_click_handler)
            drag_hint_button.setFixedSize(48, 22)
            header_row.addWidget(drag_hint_button)
            self.drag_hint_button = drag_hint_button

        layout.addLayout(header_row)

        input_row = QHBoxLayout()
        input_row.setContentsMargins(0, 0, 0, 0)
        input_row.setSpacing(8)

        line_edit.setObjectName("PathInput")
        line_edit.setMinimumHeight(38)
        input_row.addWidget(line_edit, 1)

        browse_button = QPushButton("更改")
        browse_button.setProperty("role", "subtle")
        browse_button.setObjectName("CompactButton")
        browse_button.clicked.connect(browse_handler)
        browse_button.setFixedWidth(64)
        browse_button.setFixedHeight(38)
        input_row.addWidget(browse_button)

        self.clear_button = QPushButton("清空")
        self.clear_button.setProperty("role", "ghost")
        self.clear_button.setObjectName("CompactButton")
        self.clear_button.setFixedWidth(58)
        self.clear_button.setFixedHeight(38)
        if clear_handler is not None:
            self.clear_button.clicked.connect(clear_handler)
        input_row.addWidget(self.clear_button)

        layout.addLayout(input_row)


class DropOverlay(QFrame):
    files_dropped = Signal(list)
    close_requested = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setObjectName("DropOverlay")
        self.setAcceptDrops(True)
        self.setFocusPolicy(Qt.StrongFocus)
        self._default_hint_title = "将企业微信里的二维码图片拖到这里"
        self._default_hint_text = "图片会直接复制到当前二维码目录。\n按 Esc 或点击右上角“关闭”退出此模式。"

        layout = QVBoxLayout(self)
        layout.setContentsMargins(18, 18, 18, 18)
        layout.setSpacing(14)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)

        title = QLabel("拖入二维码图片")
        title.setObjectName("DropOverlayTitle")
        header.addWidget(title)
        header.addStretch(1)

        close_button = QToolButton()
        close_button.setObjectName("DropOverlayCloseButton")
        close_button.setText("关闭")
        close_button.clicked.connect(self.close_requested.emit)
        header.addWidget(close_button)

        layout.addLayout(header)
        layout.addStretch(1)

        hint_box = QFrame()
        hint_box.setObjectName("DropOverlayBox")
        apply_soft_shadow(hint_box, blur=36, y_offset=10, alpha=20)
        hint_layout = QVBoxLayout(hint_box)
        hint_layout.setContentsMargins(24, 20, 24, 20)
        hint_layout.setSpacing(8)

        self.hint_title = QLabel(self._default_hint_title)
        self.hint_title.setObjectName("DropOverlayHintTitle")
        self.hint_title.setAlignment(Qt.AlignCenter)
        hint_layout.addWidget(self.hint_title)

        self.hint_text = QLabel(self._default_hint_text)
        self.hint_text.setObjectName("DropOverlayHintText")
        self.hint_text.setAlignment(Qt.AlignCenter)
        self.hint_text.setWordWrap(True)
        hint_layout.addWidget(self.hint_text)

        stats_row = QHBoxLayout()
        stats_row.setContentsMargins(0, 8, 0, 0)
        stats_row.setSpacing(10)

        self.last_import_card = self._build_stat_card("本次导入", "0 张")
        self.session_total_card = self._build_stat_card("本轮累计", "0 张")
        stats_row.addWidget(self.last_import_card)
        stats_row.addWidget(self.session_total_card)

        hint_layout.addLayout(stats_row)

        layout.addWidget(hint_box)
        layout.addStretch(2)

    def _build_stat_card(self, label_text, value_text):
        card = QFrame()
        card.setObjectName("DropOverlayStatCard")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(14, 12, 14, 12)
        card_layout.setSpacing(3)

        label = QLabel(label_text)
        label.setObjectName("DropOverlayStatLabel")
        card_layout.addWidget(label)

        value = QLabel(value_text)
        value.setObjectName("DropOverlayStatValue")
        card_layout.addWidget(value)

        card.value_label = value
        return card

    def reset_state(self):
        self.hint_title.setText(self._default_hint_title)
        self.hint_text.setText(self._default_hint_text)
        self.last_import_card.value_label.setText("0 张")
        self.session_total_card.value_label.setText("0 张")

    def show_drag_preview(self, count):
        self.hint_title.setText(f"松开鼠标即可导入 {count} 张二维码图片")
        self.hint_text.setText("文件会直接复制到当前二维码目录。导入完成后可继续拖入。")

    def show_idle_message(self):
        self.hint_title.setText(self._default_hint_title)
        self.hint_text.setText(self._default_hint_text)

    def show_import_result(self, last_count, total_count):
        self.last_import_card.value_label.setText(f"{last_count} 张")
        self.session_total_card.value_label.setText(f"{total_count} 张")
        self.hint_title.setText(f"本次已导入 {last_count} 张二维码图片")
        self.hint_text.setText("可继续拖入更多图片。按 Esc 或点击右上角“关闭”退出此模式。")

    def show_error_result(self, message):
        self.hint_title.setText("未导入任何二维码图片")
        self.hint_text.setText(message)

    def _extract_file_paths(self, mime_data):
        if not mime_data.hasUrls():
            return []
        return [url.toLocalFile() for url in mime_data.urls() if url.isLocalFile()]

    def dragEnterEvent(self, event):
        file_paths = self._extract_file_paths(event.mimeData())
        image_paths = [path for path in file_paths if is_supported_image_file(path)]
        if image_paths:
            self.show_drag_preview(len(image_paths))
            event.acceptProposedAction()
            return
        event.ignore()

    def dragMoveEvent(self, event):
        file_paths = self._extract_file_paths(event.mimeData())
        image_paths = [path for path in file_paths if is_supported_image_file(path)]
        if image_paths:
            self.show_drag_preview(len(image_paths))
            event.acceptProposedAction()
            return
        event.ignore()

    def dragLeaveEvent(self, event):
        self.show_idle_message()
        super().dragLeaveEvent(event)

    def dropEvent(self, event):
        file_paths = self._extract_file_paths(event.mimeData())
        image_paths = [path for path in file_paths if is_supported_image_file(path)]
        if image_paths:
            self.files_dropped.emit(image_paths)
            event.acceptProposedAction()
            return
        event.ignore()

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Escape:
            self.close_requested.emit()
            event.accept()
            return
        super().keyPressEvent(event)


class TitleBar(QFrame):
    def __init__(self, window):
        super().__init__(window)
        self._drag_active = False
        self._drag_offset = QPoint()

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._drag_active = True
            self._drag_offset = event.globalPosition().toPoint() - self.window().frameGeometry().topLeft()
            event.accept()
            return
        super().mousePressEvent(event)

    def mouseMoveEvent(self, event):
        if self._drag_active and event.buttons() & Qt.LeftButton:
            self.window().move(event.globalPosition().toPoint() - self._drag_offset)
            event.accept()
            return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton:
            self._drag_active = False
            event.accept()
            return
        super().mouseReleaseEvent(event)


class EditableDateTimeEdit(QDateTimeEdit):
    def mouseDoubleClickEvent(self, event):
        super().mouseDoubleClickEvent(event)
        self.setFocus(Qt.MouseFocusReason)
        line_edit = self.lineEdit()
        if line_edit is not None:
            line_edit.selectAll()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.worker_thread = None
        self.worker = None
        self.current_action_name = ""
        self.busy_step = 0
        self.ui_state = self._load_state()
        self.help_panel_expanded = False
        self.log_panel_expanded = False
        self.latest_ticker_message = ""
        self.pending_clear_target = None
        self.current_mode = PROCESS_MODE_PUSH
        self.current_progress_text = ""
        self.wecom_window = None
        self.wecom_webhook_combo = None
        self.wecom_webhook_edit = None
        self.wecom_webhook_note_edit = None
        self.wecom_send_interval_spin = None
        self.wecom_status_label = None
        self.push_cutoff_date_edit = None
        self.push_cutoff_field = None
        self.push_summary_template_edit = None
        self.push_settings_save_btn = None
        self.mode_combo = None
        self.mode_hint_label = None
        self.window_title_label = None

        self.busy_timer = QTimer(self)
        self.busy_timer.setInterval(360)
        self.busy_timer.timeout.connect(self._advance_busy_state)

        self.feedback_timer = QTimer(self)
        self.feedback_timer.setSingleShot(True)
        self.feedback_timer.timeout.connect(self._hide_feedback)

        self.clear_confirm_timer = QTimer(self)
        self.clear_confirm_timer.setSingleShot(True)
        self.clear_confirm_timer.setInterval(5000)
        self.clear_confirm_timer.timeout.connect(self._reset_pending_clear)

        self.window_icon = load_app_icon()
        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint)
        self.setWindowTitle("发票处理工具")
        self.setWindowTitle("发票批量下载工具")
        self.setWindowIcon(self.window_icon)
        self.setMinimumSize(MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT)
        self.resize(MIN_WINDOW_WIDTH, MIN_WINDOW_HEIGHT)

        self.setAcceptDrops(True)

        self.input_dir_edit = QLineEdit()
        self.output_dir_edit = QLineEdit()
        self.log_box = QPlainTextEdit()
        self.log_box.setReadOnly(True)
        self.log_box.setObjectName("LogBox")
        self.log_box.setFont(QFont("Consolas", 9))
        self.log_box.hide()

        self._load_settings()
        self._build_ui()
        self._apply_mode_ui()
        self._apply_styles()
        self._set_status_chip("就绪", "ready")
        self._sync_status_labels()
        self._refresh_shortcuts()
        self._restore_panel_state()
        self._seed_ticker()

    def _load_state(self):
        if not os.path.exists(UI_STATE_PATH):
            return {}
        try:
            with open(UI_STATE_PATH, "r", encoding="utf-8") as file_obj:
                return json.load(file_obj)
        except (OSError, json.JSONDecodeError):
            return {}

    def _default_wecom_webhook_entry(self):
        return {
            "url": DEFAULT_WECOM_WEBHOOK_URL,
            "note": DEFAULT_WECOM_WEBHOOK_NOTE,
        }

    def _normalize_wecom_webhook_entry(self, entry):
        if isinstance(entry, str):
            url = entry.strip()
            note = ""
        elif isinstance(entry, dict):
            url = str(entry.get("url") or "").strip()
            note = str(entry.get("note") or "").strip()
        else:
            return None

        if not url:
            return None
        if not note and url == DEFAULT_WECOM_WEBHOOK_URL:
            note = DEFAULT_WECOM_WEBHOOK_NOTE
        return {
            "url": url,
            "note": note,
        }

    def _load_wecom_webhook_entries(self):
        entries = []
        seen_urls = set()

        def add_entry(entry):
            normalized = self._normalize_wecom_webhook_entry(entry)
            if normalized is None or normalized["url"] in seen_urls:
                return
            seen_urls.add(normalized["url"])
            entries.append(normalized)

        add_entry(self._default_wecom_webhook_entry())
        for entry in self.ui_state.get("wecom_webhook_entries") or []:
            add_entry(entry)

        legacy_entry = {
            "url": self.ui_state.get("wecom_webhook_url") or "",
            "note": self.ui_state.get("wecom_webhook_note") or "",
        }
        add_entry(legacy_entry)
        return entries

    def _remember_wecom_webhook(self, url, note):
        normalized = self._normalize_wecom_webhook_entry({"url": url, "note": note})
        if normalized is None:
            return

        history = [
            entry
            for entry in (getattr(self, "wecom_webhook_entries", []) or [])
            if self._normalize_wecom_webhook_entry(entry) is not None
        ]
        history = [entry for entry in history if entry["url"] != normalized["url"]]
        history.insert(0, normalized)

        default_url = self._default_wecom_webhook_entry()["url"]
        default_entry = next((entry for entry in history if entry["url"] == default_url), None)
        history = [entry for entry in history if entry["url"] != default_url]
        if default_entry is None:
            default_entry = self._default_wecom_webhook_entry()
        history.append(default_entry)

        self.wecom_webhook_entries = history[:20]
        self.wecom_webhook_url = normalized["url"]
        self.wecom_webhook_note = normalized["note"]

    def _refresh_wecom_webhook_combo(self):
        combo = getattr(self, "wecom_webhook_combo", None)
        if combo is None:
            return

        current_url = (getattr(self, "wecom_webhook_url", "") or "").strip()
        combo.blockSignals(True)
        combo.clear()
        for entry in getattr(self, "wecom_webhook_entries", []) or []:
            label = entry["note"] or "未命名机器人"
            combo.addItem(f"{label} | {entry['url']}", entry["url"])

        if current_url:
            index = combo.findData(current_url)
            if index >= 0:
                combo.setCurrentIndex(index)
        combo.blockSignals(False)

    def _on_wecom_webhook_selected(self, index):
        if index < 0 or self.wecom_webhook_combo is None:
            return
        url = (self.wecom_webhook_combo.itemData(index) or "").strip()
        entry = next((item for item in self.wecom_webhook_entries if item["url"] == url), None)
        if entry is None:
            return
        self.wecom_webhook_url = entry["url"]
        self.wecom_webhook_note = entry["note"]
        if self.wecom_webhook_edit is not None:
            self.wecom_webhook_edit.setText(entry["url"])
        if self.wecom_webhook_note_edit is not None:
            self.wecom_webhook_note_edit.setText(entry["note"])

    def _load_settings(self):
        self.current_mode = normalize_process_mode(self.ui_state.get("process_mode", PROCESS_MODE_PUSH))
        self.input_dir_edit.setText(self.ui_state.get("input_dir") or DEFAULT_INPUT_DIR)
        self.output_dir_edit.setText(self.ui_state.get("output_dir") or DEFAULT_OUTPUT_DIR)
        self.wecom_webhook_entries = self._load_wecom_webhook_entries()
        current_entry = self._normalize_wecom_webhook_entry(
            {
                "url": self.ui_state.get("wecom_webhook_url") or "",
                "note": self.ui_state.get("wecom_webhook_note") or "",
            }
        )
        if current_entry is None:
            current_entry = self.wecom_webhook_entries[0] if self.wecom_webhook_entries else self._default_wecom_webhook_entry()
        self.wecom_webhook_url = current_entry["url"]
        self.wecom_webhook_note = current_entry["note"]
        self.wecom_send_interval_seconds = self._normalize_wecom_send_interval(
            self.ui_state.get("wecom_send_interval_seconds")
        )
        self.push_batch_cutoff_date = self._normalize_push_batch_cutoff_date(
            self.ui_state.get("push_batch_cutoff_date")
        )
        self.push_summary_template = self._normalize_push_summary_template(
            self.ui_state.get("push_summary_template")
        )
        # Panels always start collapsed so the tool opens at its compact size.
        self.help_panel_expanded = False
        self.log_panel_expanded = False

    def _save_settings(self):
        if self.push_cutoff_date_edit is not None:
            self.push_batch_cutoff_date = self._normalize_push_batch_cutoff_date(
                self.push_cutoff_date_edit.dateTime()
            )
        self.ui_state = {
            "process_mode": self.current_mode,
            "input_dir": self.input_dir(),
            "output_dir": self.output_dir(),
            "wecom_webhook_url": self._wecom_webhook(),
            "wecom_webhook_note": self._wecom_webhook_note(),
            "wecom_send_interval_seconds": self._wecom_send_interval_seconds(),
            "push_batch_cutoff_date": self._push_batch_cutoff_date(),
            "push_summary_template": self._push_summary_template(),
            "wecom_webhook_entries": list(getattr(self, "wecom_webhook_entries", []) or []),
            "setup_completed": bool(self.ui_state.get("setup_completed", False)),
        }
        try:
            with open(UI_STATE_PATH, "w", encoding="utf-8") as file_obj:
                json.dump(self.ui_state, file_obj, ensure_ascii=False, indent=2)
        except OSError:
            pass

    def _restore_panel_state(self):
        self.help_panel.hide()
        self.log_panel.hide()

    def _seed_ticker(self):
        self.latest_ticker_message = "当前状态：等待开始"
        self._render_ticker()

    def _mode_label(self, mode=None):
        mode = normalize_process_mode(mode or self.current_mode)
        if mode == PROCESS_MODE_PUSH:
            return "发票推送模式"
        return "本地下载模式"

    def _base_window_title(self):
        return "发票处理工具"

    def _apply_window_title(self, progress_text=None):
        progress = (progress_text if progress_text is not None else self.current_progress_text or "").strip()
        title = self._base_window_title()
        if progress:
            title = f"{title} [{progress}]"
        app = QApplication.instance()
        if app is not None:
            app.setApplicationDisplayName(title)
        if self.window_title_label is not None:
            self.window_title_label.setText(title)
        self.setWindowTitle(title)

    def _set_live_progress(self, progress_text):
        self.current_progress_text = (progress_text or "").strip()
        if self.current_action_name:
            button_text = self.current_action_name
            if self.current_progress_text:
                button_text = f"{button_text} {self.current_progress_text}"
            self.download_btn.setText(button_text)
            self._sync_status_labels(state="运行中", result=button_text)
        self._apply_window_title()

    def _mode_hint_text(self):
        if self.current_mode == PROCESS_MODE_PUSH:
            return "选择待发送文件夹后可批量推送；也可以直接把 PDF、二维码图片或文件夹拖到窗口里发送到企业微信群。"
        return "选择待处理文件夹和保存文件夹后可批量处理；二维码会自动下载发票，PDF 会按发票信息重命名后保存。"

    def _apply_mode_ui(self):
        mode = normalize_process_mode(self.current_mode)
        self.current_mode = mode

        if self.mode_combo is not None:
            index = self.mode_combo.findData(mode)
            if index >= 0 and self.mode_combo.currentIndex() != index:
                self.mode_combo.blockSignals(True)
                self.mode_combo.setCurrentIndex(index)
                self.mode_combo.blockSignals(False)

        if self.mode_hint_label is not None:
            self.mode_hint_label.setText(self._mode_hint_text())

        if hasattr(self, "input_path_field"):
            self.input_path_field.label.setText("待发送文件夹" if mode == PROCESS_MODE_PUSH else "待处理文件夹")

        if hasattr(self, "output_path_field"):
            label = "生成 PDF 保存文件夹" if mode == PROCESS_MODE_PUSH else "发票保存文件夹"
            self.output_path_field.label.setText(label)

        if hasattr(self, "download_btn"):
            if mode == PROCESS_MODE_PUSH:
                self.download_btn.setText("开始推送到群")
                self.download_btn.setIcon(load_icon("send", self.style().standardIcon(QStyle.SP_ArrowForward)))
            else:
                self.download_btn.setText("开始本地下载")
                self.download_btn.setIcon(load_icon("download", self.style().standardIcon(QStyle.SP_ArrowDown)))

        if self.window_title_label is not None:
            self.window_title_label.setText("发票处理工具")
        self.setWindowTitle("发票处理工具")

        if hasattr(self, "open_input_action"):
            self.open_input_action.setText("打开待发送文件夹" if mode == PROCESS_MODE_PUSH else "打开待处理文件夹")

        if hasattr(self, "open_output_action"):
            text = "打开生成 PDF 保存文件夹" if mode == PROCESS_MODE_PUSH else "打开发票保存文件夹"
            self.open_output_action.setText(text)

    def _on_mode_changed(self, index):
        if self.mode_combo is None:
            return
        mode = normalize_process_mode(self.mode_combo.itemData(index))
        if mode == self.current_mode:
            return
        self.current_mode = mode
        self._hide_feedback()
        self._apply_mode_ui()
        self._sync_status_labels(result=f"已切换到{self._mode_label()}。")
        self._push_ticker_message(f"已切换到{self._mode_label()}。")
        self._show_feedback(f"已切换到{self._mode_label()}。", "info", timeout_ms=2500)
        self._save_settings()

    def _build_ui(self):
        root = QWidget()
        root.setObjectName("RootWindow")
        self.setCentralWidget(root)

        outer = QVBoxLayout(root)
        outer.setContentsMargins(8, 8, 8, 8)
        outer.setSpacing(8)

        outer.addWidget(self._build_header())
        outer.addWidget(self._build_main_card(), 1)

        self.drop_overlay = DropOverlay(root)
        self.drop_overlay.hide()
        self.drop_overlay.files_dropped.connect(self.import_dropped_qr_files)
        self.drop_overlay.close_requested.connect(self.exit_drop_mode)
        self.drop_import_total = 0

    def _build_header(self):
        title_bar = TitleBar(self)
        title_bar.setObjectName("TitleBar")

        row = QHBoxLayout(title_bar)
        row.setContentsMargins(12, 0, 10, 0)
        row.setSpacing(8)

        title_icon = QLabel()
        title_icon.setObjectName("TitleIcon")
        title_icon.setPixmap(self.window_icon.pixmap(18, 18))
        title_icon.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        row.addWidget(title_icon)

        title_wrap = QVBoxLayout()
        title_wrap.setContentsMargins(0, 0, 0, 0)
        title_wrap.setSpacing(1)

        title = QLabel("发票批量下载工具")
        title.setObjectName("WindowTitle")
        title.setAttribute(Qt.WA_TransparentForMouseEvents, True)
        title_wrap.addWidget(title)
        self.window_title_label = title

        row.addLayout(title_wrap, 1)

        self.more_button = QToolButton()
        self.more_button.setObjectName("MoreButton")
        self.more_button.setText("")
        self.more_button.setIcon(load_icon("more", self.style().standardIcon(QStyle.SP_TitleBarUnshadeButton)))
        self.more_button.setIconSize(QSize(14, 14))
        self.more_button.setPopupMode(QToolButton.InstantPopup)
        self.more_button.setToolButtonStyle(Qt.ToolButtonIconOnly)
        self.more_button.setAutoRaise(False)
        self.more_button.setMenu(self._build_more_menu())
        row.addWidget(self.more_button)

        self.minimize_button = QToolButton()
        self.minimize_button.setObjectName("TitleBarButton")
        self.minimize_button.setProperty("traffic", "minimize")
        self.minimize_button.setText("-")
        self.minimize_button.setAutoRaise(False)
        self.minimize_button.clicked.connect(self.showMinimized)
        row.addWidget(self.minimize_button)

        self.close_button = QToolButton()
        self.close_button.setObjectName("CloseTitleBarButton")
        self.close_button.setProperty("traffic", "close")
        self.close_button.setText("x")
        self.close_button.setAutoRaise(False)
        self.close_button.clicked.connect(self.close)
        row.addWidget(self.close_button)

        return title_bar

    def _build_main_card(self):
        card = QFrame()
        self.main_card = card
        card.setObjectName("MainCard")
        apply_soft_shadow(card, blur=32, y_offset=10, alpha=22)

        layout = QVBoxLayout(card)
        layout.setContentsMargins(18, 16, 18, 16)
        layout.setSpacing(12)

        self.feedback_banner = QLabel(card)
        self.feedback_banner.setObjectName("FeedbackBanner")
        self.feedback_banner.setWordWrap(True)
        self.feedback_banner.setAlignment(Qt.AlignLeft | Qt.AlignVCenter)
        self.feedback_banner.hide()
        layout.addWidget(self.feedback_banner)

        mode_title = QLabel("工作模式")
        mode_title.setObjectName("SectionTitle")
        layout.addWidget(mode_title)

        self.mode_combo = QComboBox()
        self.mode_combo.setObjectName("PathInput")
        self.mode_combo.setMinimumHeight(38)
        self.mode_combo.addItem("发票推送模式", PROCESS_MODE_PUSH)
        self.mode_combo.addItem("本地下载模式", PROCESS_MODE_LOCAL)
        self.mode_combo.currentIndexChanged.connect(self._on_mode_changed)
        layout.addWidget(self.mode_combo)

        self.mode_hint_label = QLabel()
        self.mode_hint_label.setObjectName("HelpText")
        self.mode_hint_label.setWordWrap(True)
        self.mode_hint_label.setContentsMargins(2, 0, 2, 4)
        layout.addWidget(self.mode_hint_label)

        self.input_path_field = PathField(
            "发票二维码文件夹",
            self.input_dir_edit,
            self.choose_input_dir,
            clear_handler=self.request_clear_input_dir,
            label_click_handler=self.enter_drop_mode,
        )
        self.input_path_field.setFixedHeight(76)
        layout.addWidget(self.input_path_field)

        self.output_path_field = PathField(
            "PDF 输出文件夹",
            self.output_dir_edit,
            self.choose_output_dir,
            clear_handler=self.request_clear_output_dir,
        )
        self.output_path_field.setFixedHeight(76)
        layout.addWidget(self.output_path_field)

        self.push_mode_gap = QWidget()
        self.push_mode_gap.setFixedHeight(10)
        self.push_mode_gap.hide()
        layout.addWidget(self.push_mode_gap)

        self.push_cutoff_field = QWidget()
        push_cutoff_layout = QVBoxLayout(self.push_cutoff_field)
        push_cutoff_layout.setContentsMargins(0, 0, 0, 0)
        push_cutoff_layout.setSpacing(6)

        push_cutoff_label = QLabel("本轮发票截止时间")
        push_cutoff_label.setObjectName("FieldLabel")
        push_cutoff_layout.addWidget(push_cutoff_label)

        self.push_cutoff_date_edit = EditableDateTimeEdit()
        self.push_cutoff_date_edit.setObjectName("PathInput")
        self.push_cutoff_date_edit.setMinimumHeight(40)
        self.push_cutoff_date_edit.setDisplayFormat("yyMMdd HH:mm")
        self.push_cutoff_date_edit.setCalendarPopup(True)
        self.push_cutoff_date_edit.setAccelerated(True)
        self.push_cutoff_date_edit.setKeyboardTracking(False)
        self._set_push_cutoff_date_editor(self._push_batch_cutoff_date())
        cutoff_row = QHBoxLayout()
        cutoff_row.setContentsMargins(0, 0, 0, 0)
        cutoff_row.setSpacing(8)
        cutoff_row.addWidget(self.push_cutoff_date_edit, 1)

        self.push_settings_save_btn = QPushButton("保存截止时间")
        self.push_settings_save_btn.setProperty("role", "subtle")
        self.push_settings_save_btn.setText("保存")
        self.push_settings_save_btn.setObjectName("CompactButton")
        self.push_settings_save_btn.setFixedWidth(72)
        self.push_settings_save_btn.clicked.connect(self.save_push_cutoff_date)
        cutoff_row.addWidget(self.push_settings_save_btn, 0)

        push_summary_settings_btn = QPushButton("修改完成文案")
        push_cutoff_layout.addLayout(cutoff_row)

        self.push_cutoff_field.hide()
        layout.addWidget(self.push_cutoff_field)

        action_row = QHBoxLayout()
        action_row.setContentsMargins(0, 6, 0, 0)
        action_row.setSpacing(10)

        self.download_btn = QPushButton("开始下载发票")
        self.download_btn.setProperty("role", "primary")
        self.download_btn.setIcon(load_icon("download", self.style().standardIcon(QStyle.SP_ArrowDown)))
        self.download_btn.setIconSize(QSize(16, 16))
        self.download_btn.clicked.connect(self.start_primary_action)
        self.download_btn.setMinimumHeight(44)
        action_row.addWidget(self.download_btn, 1)

        self.toggle_log_btn = QPushButton("显示日志")
        self.toggle_log_btn.setProperty("role", "subtle")
        self.toggle_log_btn.setIcon(load_icon("log", self.style().standardIcon(QStyle.SP_FileDialogDetailedView)))
        self.toggle_log_btn.setIconSize(QSize(16, 16))
        self.toggle_log_btn.setFixedWidth(124)
        self.toggle_log_btn.clicked.connect(self.toggle_log_panel)
        action_row.addWidget(self.toggle_log_btn)

        layout.addLayout(action_row)

        status_title = QLabel("当前状态")
        status_title.setObjectName("SectionTitle")
        layout.addWidget(status_title)

        self.ticker_bar = QLabel()
        self.ticker_bar.setObjectName("TickerBar")
        self.ticker_bar.setAlignment(Qt.AlignVCenter | Qt.AlignLeft)
        layout.addWidget(self.ticker_bar)

        self.help_panel = self._build_help_panel()
        self.help_panel.hide()
        layout.addWidget(self.help_panel)

        status_meta = QHBoxLayout()
        status_meta.setContentsMargins(2, 4, 2, 0)
        status_meta.setSpacing(12)

        self.inline_status_label = QLabel()
        self.inline_status_label.setObjectName("InlineStatusMeta")
        status_meta.addWidget(self.inline_status_label, 1)

        self.inline_path_label = QLabel()
        self.inline_path_label.setObjectName("InlinePathMeta")
        self.inline_path_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        status_meta.addWidget(self.inline_path_label, 2)

        layout.addLayout(status_meta)

        self.log_panel = QFrame()
        self.log_panel.setObjectName("LogPanel")
        self.log_panel.hide()

        log_layout = QVBoxLayout(self.log_panel)
        log_layout.setContentsMargins(10, 10, 10, 10)
        log_layout.setSpacing(8)

        log_header = QHBoxLayout()
        log_header.setContentsMargins(0, 0, 0, 0)

        log_title = QLabel("处理记录")
        log_title.setObjectName("SectionTitle")
        log_header.addWidget(log_title)
        log_header.addStretch(1)

        clear_log_btn = QPushButton("清空")
        clear_log_btn.setProperty("role", "subtle")
        clear_log_btn.setObjectName("CompactButton")
        clear_log_btn.setIcon(load_icon("clear", self.style().standardIcon(QStyle.SP_DialogResetButton)))
        clear_log_btn.setIconSize(QSize(14, 14))
        clear_log_btn.setFixedWidth(82)
        clear_log_btn.clicked.connect(self.clear_log)
        log_header.addWidget(clear_log_btn)

        log_layout.addLayout(log_header)

        self.log_box.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.log_box.setMaximumBlockCount(4000)
        self.log_box.show()
        log_layout.addWidget(self.log_box)

        layout.addWidget(self.log_panel, 1)
        return card

    def _build_help_panel(self):
        panel = QFrame()
        panel.setObjectName("HelpPanel")

        layout = QVBoxLayout(panel)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        header = QHBoxLayout()
        header.setContentsMargins(0, 0, 0, 0)

        title = QLabel("使用帮助")
        title.setObjectName("SectionTitle")
        header.addWidget(title)
        header.addStretch(1)

        hide_btn = QPushButton("收起")
        hide_btn.setProperty("role", "subtle")
        hide_btn.setObjectName("CompactButton")
        hide_btn.setFixedWidth(82)
        hide_btn.clicked.connect(lambda: self.toggle_help_panel(force_visible=False))
        header.addWidget(hide_btn)

        layout.addLayout(header)

        help_text = QLabel(load_guide_text())
        help_text.setObjectName("HelpText")
        help_text.setTextFormat(Qt.PlainText)
        help_text.setWordWrap(True)
        help_text.setTextInteractionFlags(Qt.TextSelectableByMouse)
        layout.addWidget(help_text)
        return panel

    def _build_more_menu(self):
        menu = QMenu(self)

        self.help_action = QAction("显示帮助", self)
        self.help_action.setIcon(load_icon("help", self.style().standardIcon(QStyle.SP_DialogHelpButton)))
        self.help_action.triggered.connect(self.toggle_help_panel)
        menu.addAction(self.help_action)

        open_input_action = QAction("打开二维码目录", self)
        open_input_action.setIcon(load_icon("folder", self.style().standardIcon(QStyle.SP_DirOpenIcon)))
        open_input_action.triggered.connect(self.open_input_dir)
        menu.addAction(open_input_action)
        self.open_input_action = open_input_action

        open_output_action = QAction("打开输出目录", self)
        open_output_action.setIcon(load_icon("folder", self.style().standardIcon(QStyle.SP_DirOpenIcon)))
        open_output_action.triggered.connect(self.open_output_dir)
        menu.addAction(open_output_action)
        self.open_output_action = open_output_action

        wecom_action = QAction("企业微信 Webhook 设置", self)
        wecom_action.setIcon(load_icon("send", self.style().standardIcon(QStyle.SP_ArrowForward)))
        wecom_action.triggered.connect(self.show_wecom_window)
        menu.addAction(wecom_action)

        menu.addSeparator()

        exit_action = QAction("退出", self)
        exit_action.setIcon(load_icon("exit", self.style().standardIcon(QStyle.SP_DialogCloseButton)))
        exit_action.triggered.connect(self.close)
        menu.addAction(exit_action)
        return menu

    def _apply_styles(self):
        self.setStyleSheet(
            """
            QWidget {
                background: #eef1f5;
                color: #1d1d1f;
                font-family: "Segoe UI Variable", "Segoe UI", "Microsoft YaHei UI", sans-serif;
                font-size: 13px;
            }
            QMainWindow {
                background: #e9edf3;
            }
            #RootWindow {
                background: qlineargradient(
                    x1: 0, y1: 0, x2: 0, y2: 1,
                    stop: 0 #f6f8fb,
                    stop: 1 #edf1f5
                );
                border: none;
                border-radius: 16px;
            }
            #TitleBar {
                min-height: 40px;
                background: rgba(255, 255, 255, 0.54);
                border: none;
                border-radius: 12px;
            }
            #TitleIcon {
                background: transparent;
                min-width: 18px;
                max-width: 18px;
            }
            #WindowTitle {
                font-size: 15px;
                font-weight: 600;
                color: #1d1d1f;
                background: transparent;
            }
            #DropOverlay {
                background: rgba(236, 240, 245, 0.88);
                border: 1px solid rgba(60, 60, 67, 0.12);
                border-radius: 18px;
            }
            #DropOverlayTitle {
                background: transparent;
                color: #1d1d1f;
                font-size: 15px;
                font-weight: 600;
            }
            #DropOverlayCloseButton {
                min-height: 30px;
                padding: 0 12px;
                border-radius: 15px;
                border: 1px solid rgba(60, 60, 67, 0.14);
                background: rgba(255, 255, 255, 0.90);
                color: #4e5157;
                font-size: 12px;
                font-weight: 600;
            }
            #DropOverlayCloseButton:hover {
                background: rgba(255, 255, 255, 1.0);
                border: 1px solid rgba(60, 60, 67, 0.20);
                color: #1d1d1f;
            }
            #DropOverlayBox {
                background: rgba(255, 255, 255, 0.86);
                border: 1px solid rgba(10, 132, 255, 0.18);
                border-radius: 20px;
            }
            #DropOverlayHintTitle {
                background: transparent;
                color: #0a84ff;
                font-size: 20px;
                font-weight: 600;
            }
            #DropOverlayHintText {
                background: transparent;
                color: #6e6e73;
                font-size: 13px;
                line-height: 1.5;
            }
            #DropOverlayStatCard {
                background: rgba(246, 249, 255, 0.94);
                border: 1px solid rgba(10, 132, 255, 0.12);
                border-radius: 14px;
            }
            #DropOverlayStatLabel {
                background: transparent;
                color: #8e8e93;
                font-size: 12px;
                font-weight: 600;
            }
            #DropOverlayStatValue {
                background: transparent;
                color: #0a84ff;
                font-size: 22px;
                font-weight: 600;
            }
            #MoreButton {
                min-width: 30px;
                max-width: 30px;
                min-height: 30px;
                max-height: 30px;
                padding: 0;
                border-radius: 15px;
                border: none;
                background: rgba(255, 255, 255, 0.74);
                color: #6e6e73;
                font-size: 13px;
                font-weight: 600;
            }
            #TitleBarButton, #CloseTitleBarButton {
                min-width: 14px;
                max-width: 14px;
                min-height: 14px;
                max-height: 14px;
                padding: 0;
                border-radius: 7px;
                border: none;
                font-size: 9px;
                font-weight: 700;
                color: rgba(29, 29, 31, 0.78);
            }
            #MoreButton:hover {
                background: rgba(255, 255, 255, 0.96);
                color: #1d1d1f;
            }
            #TitleBarButton {
                background: #f2c94c;
            }
            #TitleBarButton[traffic="minimize"]:hover {
                background: #f0bd2f;
                border: none;
                color: rgba(29, 29, 31, 0.88);
            }
            #CloseTitleBarButton {
                background: #ff6b6b;
            }
            #CloseTitleBarButton:hover {
                background: #ff5757;
                border: none;
                color: rgba(29, 29, 31, 0.88);
            }
            #FeedbackBanner {
                min-height: 36px;
                padding: 0 12px;
                border-radius: 12px;
                border: none;
                background: rgba(255, 255, 255, 0.58);
                color: #4e5157;
                font-size: 12px;
                font-weight: 600;
            }
            #FeedbackBanner[tone="success"] {
                background: rgba(46, 155, 95, 0.10);
                border: 1px solid rgba(46, 155, 95, 0.18);
                color: #2e9b5f;
            }
            #FeedbackBanner[tone="error"] {
                background: rgba(196, 73, 61, 0.09);
                border: 1px solid rgba(196, 73, 61, 0.16);
                color: #c4493d;
            }
            #FeedbackBanner[tone="info"] {
                background: rgba(10, 132, 255, 0.10);
                border: 1px solid rgba(10, 132, 255, 0.16);
                color: #0a84ff;
            }
            #TickerBar {
                min-height: 36px;
                padding: 0 12px;
                border-radius: 12px;
                border: none;
                background: rgba(255, 255, 255, 0.58);
                color: #4e5157;
                font-size: 12px;
            }
            #InlineStatusMeta, #InlinePathMeta {
                background: transparent;
                color: #8e8e93;
                font-size: 11px;
                font-weight: 500;
                padding: 0 2px;
            }
            #MainCard {
                background: rgba(255, 255, 255, 0.82);
                border: none;
                border-radius: 16px;
            }
            #PathField {
                background: transparent;
                border: none;
                border-radius: 0;
            }
            #LogPanel, #HelpPanel {
                background: rgba(255, 255, 255, 0.42);
                border: none;
                border-radius: 14px;
            }
            #FieldLabel, #SectionTitle {
                background: transparent;
                font-weight: 600;
                color: #4f5661;
                font-size: 12px;
            }
            #FieldHintButton {
                min-height: 24px;
                max-height: 24px;
                padding: 0 10px;
                border-radius: 12px;
                border: none;
                background: rgba(10, 132, 255, 0.08);
                color: #0a84ff;
                font-size: 11px;
                font-weight: 600;
            }
            #FieldHintButton:hover {
                background: rgba(10, 132, 255, 0.12);
            }
            QLineEdit, QDateTimeEdit, #PathInput {
                min-height: 38px;
                padding: 0 14px;
                border: 1px solid rgba(60, 60, 67, 0.16);
                border-radius: 10px;
                background: rgba(255, 255, 255, 0.96);
                color: #1d1d1f;
                selection-background-color: rgba(10, 132, 255, 0.18);
            }
            QLineEdit:focus, QDateTimeEdit:focus, #PathInput:focus {
                border: 1px solid #0a84ff;
                background: rgba(255, 255, 255, 1.0);
            }
            #PushSummaryTemplateInput {
                background: rgba(255, 255, 255, 0.96);
                color: #1d1d1f;
                border: 1px solid rgba(60, 60, 67, 0.16);
                border-radius: 10px;
                padding: 10px 12px;
                selection-background-color: rgba(10, 132, 255, 0.18);
            }
            #PushSummaryTemplateInput:focus {
                border: 1px solid #0a84ff;
                background: rgba(255, 255, 255, 1.0);
            }
            QPushButton, QToolButton {
                min-height: 38px;
                padding: 0 14px;
                border-radius: 10px;
                border: 1px solid rgba(60, 60, 67, 0.16);
                background: rgba(255, 255, 255, 0.90);
                color: #2f3136;
                font-weight: 600;
            }
            QPushButton[role="primary"] {
                background: #0a84ff;
                border: 1px solid #0a84ff;
                color: #ffffff;
                font-size: 13px;
                font-weight: 600;
            }
            QPushButton[role="primary"]:hover {
                background: #0077ed;
                border: 1px solid #0077ed;
            }
            QPushButton[role="primary"]:pressed {
                background: #006fe8;
                border: 1px solid #006fe8;
            }
            QPushButton[role="primary"][busy="true"] {
                background: #0077ed;
                border: 1px solid #0077ed;
                padding-left: 18px;
                padding-right: 18px;
            }
            QPushButton[role="subtle"], QPushButton#CompactButton {
                background: rgba(255, 255, 255, 0.90);
                color: #4e5157;
            }
            QPushButton[role="ghost"] {
                background: transparent;
                border: none;
                color: #8e8e93;
            }
            QPushButton[role="ghost"]:hover {
                background: rgba(255, 255, 255, 0.52);
                color: #4e5157;
            }
            QPushButton:hover, QToolButton:hover {
                background: rgba(255, 255, 255, 1.0);
                border: 1px solid rgba(60, 60, 67, 0.22);
            }
            QPushButton:pressed, QToolButton:pressed {
                background: rgba(241, 244, 248, 1.0);
                border: 1px solid rgba(60, 60, 67, 0.24);
            }
            #CompactButton {
                font-size: 12px;
                font-weight: 600;
            }
            QPushButton:disabled, QToolButton:disabled {
                color: #a1a1aa;
                background: rgba(248, 249, 251, 0.8);
                border: 1px solid rgba(60, 60, 67, 0.08);
            }
            QMenu {
                background: rgba(255, 255, 255, 0.98);
                border: 1px solid rgba(60, 60, 67, 0.12);
                padding: 8px;
                border-radius: 12px;
            }
            QMenu::item {
                padding: 8px 28px 8px 12px;
                border-radius: 8px;
                color: #2f3136;
            }
            QMenu::item:selected {
                background: rgba(10, 132, 255, 0.10);
                color: #0a84ff;
            }
            QToolButton::menu-indicator {
                image: none;
                width: 0px;
            }
            #LogBox {
                background: rgba(244, 246, 249, 0.92);
                color: #3a3d43;
                border: 1px solid rgba(60, 60, 67, 0.12);
                border-radius: 12px;
                padding: 10px;
            }
            #HelpText {
                background: transparent;
                color: #6e6e73;
                font-size: 12px;
                line-height: 1.5;
                padding: 0 2px;
            }
            """
        )

    def _set_status_chip(self, text, tone):
        return None

    def _set_primary_busy(self, busy):
        self.download_btn.setProperty("busy", "true" if busy else "false")
        self.download_btn.style().unpolish(self.download_btn)
        self.download_btn.style().polish(self.download_btn)
        self.download_btn.update()

    def _push_ticker_message(self, message):
        text = " ".join(part.strip() for part in str(message).splitlines() if part.strip())
        if not text:
            return
        self.latest_ticker_message = text
        self._render_ticker()

    def _render_ticker(self):
        if not self.latest_ticker_message:
            self.ticker_bar.setText("当前状态：等待开始")
            return
        self.ticker_bar.setText(self.latest_ticker_message)

    def _position_feedback_banner(self):
        if not hasattr(self, "feedback_banner") or not hasattr(self, "main_card"):
            return
        width = max(220, self.main_card.width() - 36)
        self.feedback_banner.setFixedWidth(width)
        self.feedback_banner.adjustSize()

    def _show_feedback(self, message, tone, timeout_ms=5000):
        self.feedback_banner.setText(message)
        self.feedback_banner.setProperty("tone", tone)
        self.feedback_banner.style().unpolish(self.feedback_banner)
        self.feedback_banner.style().polish(self.feedback_banner)
        self._position_feedback_banner()
        self.feedback_banner.show()
        if timeout_ms > 0:
            self.feedback_timer.start(timeout_ms)
        else:
            self.feedback_timer.stop()

    def _hide_feedback(self):
        self.feedback_timer.stop()
        self.feedback_banner.hide()

    def enter_drop_mode(self):
        self.drop_import_total = 0
        self.drop_overlay.reset_state()
        self.drop_overlay.setGeometry(self.centralWidget().rect())
        self.drop_overlay.show()
        self.drop_overlay.raise_()
        self.drop_overlay.setFocus()
        self._show_feedback("拖入模式已开启：将二维码图片拖到窗口内即可导入。", "info", timeout_ms=0)
        self._push_ticker_message("拖入模式已开启。")

    def exit_drop_mode(self):
        if hasattr(self, "drop_overlay"):
            self.drop_overlay.hide()
        self._hide_feedback()
        self._push_ticker_message("已退出拖入模式。")

    def import_dropped_qr_files(self, file_paths):
        copied_paths = copy_images_to_directory(file_paths, self.input_dir())
        if not copied_paths:
            self.drop_overlay.show_error_result("未识别到可导入的二维码图片，请拖入 PNG、JPG、JPEG、BMP 或 WEBP 图片。")
            self._show_feedback("未识别到可导入的二维码图片。", "error", timeout_ms=4000)
            self._push_ticker_message("未识别到可导入的二维码图片。")
            self.drop_overlay.raise_()
            self.drop_overlay.setFocus()
            return

        count = len(copied_paths)
        self.drop_import_total += count
        self.drop_overlay.show_import_result(count, self.drop_import_total)
        self._show_feedback(
            f"本次导入 {count} 张，本轮累计 {self.drop_import_total} 张，可继续拖入。",
            "success",
            timeout_ms=4500,
        )
        self._push_ticker_message(f"本次导入 {count} 张，本轮累计 {self.drop_import_total} 张。")
        self._sync_status_labels(result=f"本次导入 {count} 张，本轮累计 {self.drop_import_total} 张。")
        self.drop_overlay.raise_()
        self.drop_overlay.setFocus()

    def _sync_status_labels(self, state="就绪", result="等待操作。"):
        self.inline_status_label.setText(f"当前状态：{state} · {result}")
        self.inline_path_label.setText(f"PDF 输出位置：{self.output_dir()}")

    def _failed_log_path(self):
        return os.path.join(self.output_dir(), "failed.txt")

    def _index_file_path(self):
        return os.path.join(self.output_dir(), "processed_index.json")

    def _wecom_webhook(self):
        return (getattr(self, "wecom_webhook_url", "") or "").strip()

    def _wecom_webhook_note(self):
        return (getattr(self, "wecom_webhook_note", "") or "").strip()

    def _normalize_wecom_send_interval(self, value):
        try:
            interval = float(value)
        except (TypeError, ValueError):
            interval = quick_send.DEFAULT_WECOM_SEND_INTERVAL_SECONDS
        return max(0.0, interval)

    def _wecom_send_interval_seconds(self):
        return self._normalize_wecom_send_interval(getattr(self, "wecom_send_interval_seconds", None))

    def _wecom_send_interval_notice(self, interval=None):
        interval = self._normalize_wecom_send_interval(
            self._wecom_send_interval_seconds() if interval is None else interval
        )
        if interval < 2.0:
            return "不推荐低于 2.0 秒，容易触发微信限流。"
        return "建议发送间隔不低于 2.0 秒。"

    def _normalize_push_batch_cutoff_date(self, value):
        if isinstance(value, QDateTime):
            if value.isValid():
                return value.toString("yyMMddHHmm")
            return quick_send.default_batch_cutoff_date()
        if isinstance(value, QDate):
            if value.isValid():
                return value.toString("yyMMdd")
            return quick_send.default_batch_cutoff_date()
        try:
            return quick_send.normalize_batch_cutoff_date(value)
        except ValueError:
            return quick_send.default_batch_cutoff_date()

    def _push_batch_cutoff_date(self):
        return self._normalize_push_batch_cutoff_date(getattr(self, "push_batch_cutoff_date", None))

    def _current_push_batch_cutoff_date(self):
        if self.push_cutoff_date_edit is not None:
            return self._normalize_push_batch_cutoff_date(self.push_cutoff_date_edit.dateTime())
        return self._push_batch_cutoff_date()

    def _normalize_push_summary_template(self, value):
        return quick_send.normalize_batch_summary_template(value)

    def _push_summary_template(self):
        return self._normalize_push_summary_template(getattr(self, "push_summary_template", None))

    def _push_cutoff_qdatetime(self, value=None):
        normalized = self._normalize_push_batch_cutoff_date(
            value if value is not None else getattr(self, "push_batch_cutoff_date", None)
        )
        if len(normalized) == 6:
            normalized = f"{normalized}2359"
        date_time_value = QDateTime.fromString(f"20{normalized}", "yyyyMMddHHmm")
        if date_time_value.isValid():
            return date_time_value
        return QDateTime.currentDateTime()

    def _set_push_cutoff_date_editor(self, value=None):
        if self.push_cutoff_date_edit is None:
            return
        date_time_value = self._push_cutoff_qdatetime(value)
        self.push_cutoff_date_edit.blockSignals(True)
        self.push_cutoff_date_edit.setDateTime(date_time_value)
        self.push_cutoff_date_edit.blockSignals(False)

    def _commit_push_cutoff_date(self, save_settings=True, show_feedback=False):
        cutoff_value = getattr(self, "push_batch_cutoff_date", None)
        if self.push_cutoff_date_edit is not None:
            cutoff_value = self.push_cutoff_date_edit.dateTime()
        normalized_cutoff = self._normalize_push_batch_cutoff_date(cutoff_value)
        self.push_batch_cutoff_date = normalized_cutoff
        self._set_push_cutoff_date_editor(normalized_cutoff)
        if save_settings:
            self._save_settings()
        if show_feedback:
            display_cutoff = quick_send.format_batch_cutoff_date(normalized_cutoff)
            self._show_feedback("截止时间已保存。", "success", timeout_ms=2500)
            self._push_ticker_message(f"截止时间已保存：{display_cutoff}")
        return normalized_cutoff

    def _commit_push_message_settings(self, save_settings=True, show_feedback=False):
        normalized_cutoff = self._commit_push_cutoff_date(save_settings=False, show_feedback=False)

        template_value = getattr(self, "push_summary_template", None)
        if self.push_summary_template_edit is not None:
            template_value = self.push_summary_template_edit.toPlainText().strip()
        normalized_template = self._normalize_push_summary_template(template_value)
        self.push_summary_template = normalized_template
        if self.push_summary_template_edit is not None:
            current_text = self.push_summary_template_edit.toPlainText().strip()
            if current_text != normalized_template:
                self.push_summary_template_edit.blockSignals(True)
                self.push_summary_template_edit.setPlainText(normalized_template)
                self.push_summary_template_edit.blockSignals(False)

        if save_settings:
            self._save_settings()
        if show_feedback:
            display_cutoff = quick_send.format_batch_cutoff_date(normalized_cutoff)
            self._show_feedback("推送设置已保存。", "success", timeout_ms=2500)
            self._push_ticker_message(
                f"推送设置已保存：截止 {display_cutoff}，完成提醒文案已更新。"
            )
        return normalized_cutoff, normalized_template

    def save_push_cutoff_date(self):
        self._commit_push_cutoff_date(save_settings=True, show_feedback=True)

    def save_push_message_settings(self):
        self._commit_push_message_settings(save_settings=True, show_feedback=True)

    def _refresh_shortcuts(self):
        return None

    def _open_path_or_feedback(self, path, missing_message):
        if not os.path.exists(path):
            self._show_feedback(missing_message, "info", timeout_ms=3500)
            self._refresh_shortcuts()
            return
        QDesktopServices.openUrl(QUrl.fromLocalFile(path))

    def input_dir(self):
        return os.path.abspath(self.input_dir_edit.text().strip() or DEFAULT_INPUT_DIR)

    def output_dir(self):
        return os.path.abspath(self.output_dir_edit.text().strip() or DEFAULT_OUTPUT_DIR)

    def _dropped_local_paths(self, mime_data):
        if not mime_data.hasUrls():
            return []
        return [url.toLocalFile() for url in mime_data.urls() if url.isLocalFile()]

    def _dropped_fast_send_paths(self, mime_data):
        return quick_send.collect_supported_push_input_files(self._dropped_local_paths(mime_data))

    def _ensure_wecom_webhook(self):
        webhook = self._wecom_webhook()
        if webhook:
            return webhook
        self._show_feedback("请先在“更多”里设置企业微信 Webhook。", "info", timeout_ms=3500)
        self.show_wecom_window()
        return ""

    def choose_input_dir(self):
        path = QFileDialog.getExistingDirectory(self, "选择二维码图片目录", self.input_dir())
        if path:
            self.input_dir_edit.setText(path)
            self._reset_pending_clear()
            self._save_settings()
            self._sync_status_labels(result="已更新二维码目录。")
            return True
        return False

    def choose_output_dir(self):
        path = QFileDialog.getExistingDirectory(self, "选择发票输出目录", self.output_dir())
        if path:
            self.output_dir_edit.setText(path)
            self._reset_pending_clear()
            self._save_settings()
            self._refresh_shortcuts()
            self._sync_status_labels(result="已更新输出目录。")
            return True
        return False

    def open_input_dir(self):
        self.open_folder(self.input_dir())

    def open_output_dir(self):
        self.open_folder(self.output_dir())

    def open_failed_log(self):
        self._open_path_or_feedback(self._failed_log_path(), "当前还没有失败清单。")

    def open_index_file(self):
        self._open_path_or_feedback(self._index_file_path(), "当前还没有处理索引。")

    def save_wecom_settings(self):
        webhook_url = self._wecom_webhook()
        webhook_note = self._wecom_webhook_note()
        if self.wecom_webhook_edit is not None:
            webhook_url = self.wecom_webhook_edit.text().strip()
        if self.wecom_webhook_note_edit is not None:
            webhook_note = self.wecom_webhook_note_edit.text().strip()

        if not webhook_url:
            self._show_feedback("请先填写企业微信 Webhook 地址。", "error", timeout_ms=3000)
            return

        self._remember_wecom_webhook(webhook_url, webhook_note or "未命名机器人")
        self._refresh_wecom_webhook_combo()
        self._save_settings()
        self._show_feedback(f"企业微信机器人已保存：{self._wecom_webhook_note() or '未命名机器人'}", "success", timeout_ms=2500)

    def save_wecom_settings_and_close(self):
        webhook_url = self._wecom_webhook()
        webhook_note = self._wecom_webhook_note()
        if self.wecom_webhook_edit is not None:
            webhook_url = self.wecom_webhook_edit.text().strip()
        if self.wecom_webhook_note_edit is not None:
            webhook_note = self.wecom_webhook_note_edit.text().strip()

        if not webhook_url:
            if self.wecom_status_label is not None:
                self.wecom_status_label.setText("请先填写企业微信 Webhook 地址。")
            self._show_feedback("请先填写企业微信 Webhook 地址。", "error", timeout_ms=3000)
            return

        self._remember_wecom_webhook(webhook_url, webhook_note or "未命名机器人")
        self._refresh_wecom_webhook_combo()
        self._save_settings()
        if self.wecom_status_label is not None:
            self.wecom_status_label.setText(f"已保存：{self._wecom_webhook_note() or '未命名机器人'}")
        self._show_feedback(f"已保存企业微信机器人：{self._wecom_webhook_note() or '未命名机器人'}", "success", timeout_ms=2500)
        self._push_ticker_message(f"已保存企业微信机器人：{self._wecom_webhook_note() or '未命名机器人'}")
        if self.wecom_window is not None:
            self.wecom_window.hide()
        self.raise_()
        self.activateWindow()

    def show_wecom_window(self):
        if self.wecom_window is None:
            window = QWidget(None, Qt.Window)
            window.setWindowTitle("企业微信 Webhook 设置")
            window.setWindowIcon(self.window_icon)
            window.resize(640, 250)

            layout = QVBoxLayout(window)
            layout.setContentsMargins(12, 12, 12, 12)
            layout.setSpacing(8)

            title = QLabel("企业微信 Webhook")
            title.setObjectName("SectionTitle")
            layout.addWidget(title)

            hint = QLabel(
                "把群机器人的 Webhook 地址填在这里。之后可直接把 PDF、二维码图片，或它们所在的文件夹拖到主窗口，程序会生成重命名后的 PDF 并发送到企业微信。"
            )
            hint.setObjectName("HelpText")
            hint.setWordWrap(True)
            layout.addWidget(hint)

            saved_combo = QComboBox()
            saved_combo.setObjectName("PathInput")
            saved_combo.currentIndexChanged.connect(self._on_wecom_webhook_selected)
            layout.addWidget(saved_combo)

            webhook_edit = QLineEdit()
            webhook_edit.setObjectName("PathInput")
            webhook_edit.setPlaceholderText("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...")
            webhook_edit.setText(self._wecom_webhook())
            layout.addWidget(webhook_edit)

            note_edit = QLineEdit()
            note_edit.setObjectName("PathInput")
            note_edit.setPlaceholderText("机器人备注，例如：测试机器人")
            note_edit.setText(self._wecom_webhook_note())
            layout.addWidget(note_edit)

            status_label = QLabel("")
            status_label.setObjectName("HelpText")
            status_label.setWordWrap(True)
            layout.addWidget(status_label)

            buttons = QHBoxLayout()
            buttons.setContentsMargins(0, 0, 0, 0)
            buttons.addStretch(1)

            save_btn = QPushButton("保存")
            save_btn.setProperty("role", "subtle")
            save_btn.setObjectName("CompactButton")
            save_btn.setFixedWidth(82)
            save_btn.clicked.connect(self.save_wecom_settings_and_close)
            buttons.addWidget(save_btn)

            layout.addLayout(buttons)

            self.wecom_window = window
            self.wecom_webhook_combo = saved_combo
            self.wecom_webhook_edit = webhook_edit
            self.wecom_webhook_note_edit = note_edit
            self.wecom_status_label = status_label
        else:
            self._refresh_wecom_webhook_combo()
            self.wecom_webhook_edit.setText(self._wecom_webhook())
            self.wecom_webhook_note_edit.setText(self._wecom_webhook_note())
            if self.wecom_status_label is not None:
                self.wecom_status_label.setText("")

        self._refresh_wecom_webhook_combo()
        self.wecom_window.show()
        self.wecom_window.raise_()
        self.wecom_window.activateWindow()

    def open_folder(self, path):
        os.makedirs(path, exist_ok=True)
        self._refresh_shortcuts()
        QDesktopServices.openUrl(QUrl.fromLocalFile(path))

    def clear_log(self):
        self.log_box.clear()
        self._sync_status_labels(result="日志已清空。")
        self._show_feedback("日志已清空。", "info", timeout_ms=2500)
        self._push_ticker_message("日志已清空。")

    def _update_clear_buttons(self):
        input_text = "确认清空" if self.pending_clear_target == "input" else "清空"
        output_text = "确认清空" if self.pending_clear_target == "output" else "清空"
        self.input_path_field.clear_button.setText(input_text)
        self.output_path_field.clear_button.setText(output_text)

    def _reset_pending_clear(self):
        self.pending_clear_target = None
        self.clear_confirm_timer.stop()
        if hasattr(self, "input_path_field") and hasattr(self, "output_path_field"):
            self._update_clear_buttons()

    def _prepare_clear_directory(self, target_key, folder_label):
        if self.worker_thread is not None:
            self._show_feedback("当前已有任务在运行，请等待完成后再操作。", "info", timeout_ms=3500)
            return False

        if self.pending_clear_target != target_key:
            self.pending_clear_target = target_key
            self._update_clear_buttons()
            self.clear_confirm_timer.start()
            self._show_feedback(
                f"再次点击“清空”即可清空{folder_label}中的全部内容。",
                "error",
                timeout_ms=5000,
            )
            self._push_ticker_message(f"等待确认：清空{folder_label}。")
            return False

        self._reset_pending_clear()
        return True

    def request_clear_input_dir(self):
        if not self._prepare_clear_directory("input", "二维码目录"):
            return
        self.start_worker(
            "清空二维码目录",
            lambda: self.run_clear_directory_job(self.input_dir()),
        )

    def request_clear_output_dir(self):
        if not self._prepare_clear_directory("output", "输出目录"):
            return
        self.start_worker(
            "清空输出目录",
            lambda: self.run_clear_directory_job(self.output_dir()),
        )

    def toggle_help_panel(self, force_visible=None):
        visible = force_visible if force_visible is not None else not self.help_panel_expanded
        self.help_panel_expanded = visible
        self.help_panel.show() if visible else self.help_panel.hide()
        self.help_action.setText("隐藏帮助" if visible else "显示帮助")
        self._save_settings()

    def toggle_log_panel(self, force_visible=None):
        visible = force_visible if force_visible is not None else not self.log_panel_expanded
        self.log_panel_expanded = visible
        self.log_panel.show() if visible else self.log_panel.hide()
        self.toggle_log_btn.setText("隐藏日志" if visible else "显示日志")
        icon_name = "collapse" if visible else "log"
        fallback = self.style().standardIcon(
            QStyle.SP_TitleBarShadeButton if visible else QStyle.SP_FileDialogDetailedView
        )
        self.toggle_log_btn.setIcon(load_icon(icon_name, fallback))
        if visible and self.height() < LOG_OPEN_HEIGHT:
            self.resize(self.width(), LOG_OPEN_HEIGHT)
        elif not visible and self.height() > MIN_WINDOW_HEIGHT:
            self.resize(self.width(), MIN_WINDOW_HEIGHT)
        self.centralWidget().layout().activate()
        self._save_settings()

    def _advance_busy_state(self):
        dots = "." * (self.busy_step % 3 + 1)
        if self.current_action_name:
            button_text = self.current_action_name
            if self.current_progress_text:
                button_text = f"{button_text} {self.current_progress_text}"
            self.download_btn.setText(f"{button_text}{dots}")
        self.busy_step += 1

    def set_running(self, running, message):
        self.more_button.setDisabled(running)
        self.download_btn.setDisabled(False)
        self._set_primary_busy(running)

        if running:
            self._hide_feedback()
            self.busy_step = 0
            self.current_progress_text = ""
            self._apply_window_title()
            self.busy_timer.start()
            self._set_status_chip("运行中", "running")
            self._advance_busy_state()
            self._sync_status_labels(state="运行中", result=message)
            return

        self.busy_timer.stop()
        self.current_progress_text = ""
        self._apply_window_title()
        self._apply_mode_ui()
        self.download_btn.setText("开始下载")
        self._set_status_chip("就绪", "ready")
        self._sync_status_labels(state="就绪", result=message)

    def start_download(self):
        self.start_worker(
            "下载发票",
            lambda: self.run_download_job(self.input_dir(), self.output_dir()),
        )

    def start_fast_send_from_paths(self, paths):
        webhook = self._ensure_wecom_webhook()
        if not webhook:
            return

        file_paths = quick_send.collect_supported_push_input_files(paths)
        if not file_paths:
            self._show_feedback("未识别到可快速发送的 PDF 或二维码图片。", "error", timeout_ms=4000)
            self._push_ticker_message("未识别到可快速发送的 PDF 或二维码图片。")
            return

        self.start_worker(
            "快速发送到企业微信",
            lambda: self.run_fast_send_job(file_paths, self.output_dir(), webhook),
        )

    def start_primary_action(self):
        if self.current_mode == PROCESS_MODE_PUSH:
            webhook = self._ensure_wecom_webhook()
            if not webhook:
                return
            self.start_worker(
                "发票推送到群",
                lambda: self.run_fast_send_job([self.input_dir()], self.output_dir(), webhook),
            )
            return

        self.start_worker(
            "本地下载发票",
            lambda: self.run_download_job([self.input_dir()], self.output_dir()),
        )

    def start_process_from_paths(self, paths):
        if self.current_mode == PROCESS_MODE_PUSH:
            file_paths = quick_send.collect_supported_push_input_files(paths)
        else:
            file_paths = quick_send.collect_supported_input_files(paths)
        if not file_paths:
            self._show_feedback("未识别到可处理的 PDF 或二维码图片。", "error", timeout_ms=4000)
            self._push_ticker_message("未识别到可处理的 PDF 或二维码图片。")
            return

        if self.current_mode == PROCESS_MODE_PUSH:
            self.start_fast_send_from_paths(file_paths)
            return

        self.start_worker(
            "本地下载发票",
            lambda: self.run_download_job(file_paths, self.output_dir()),
        )

    def start_cleanup(self, dry_run):
        action_name = "预览清理重复 PDF" if dry_run else "清理重复 PDF"
        self.start_worker(
            action_name,
            lambda: self.run_cleanup_job(self.output_dir(), dry_run),
        )

    def start_worker(self, action_name, task):
        if self.worker_thread is not None:
            self._show_feedback("当前已有任务在运行，请等待完成后再操作。", "info", timeout_ms=3500)
            return

        self._reset_pending_clear()
        self._save_settings()
        self._refresh_shortcuts()

        self.current_action_name = action_name
        self.log_box.appendPlainText(f"\n=== {action_name} ===\n")
        self._push_ticker_message(f"{action_name}已开始。")
        self.set_running(True, f"{action_name}中...")

        self.worker_thread = QThread(self)
        self.worker = TaskWorker(action_name, task)
        self.worker.moveToThread(self.worker_thread)
        self.worker.log.connect(self.append_log)
        self.worker.finished.connect(self.handle_worker_finished)
        self.worker_thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.worker_thread.quit)
        self.worker.finished.connect(self.worker.deleteLater)
        self.worker_thread.finished.connect(self.worker_thread.deleteLater)
        self.worker_thread.finished.connect(self._clear_worker_refs)
        self.worker_thread.start()

    def run_download_job(self, paths, output_dir):
        return quick_send.process_inputs_locally(paths, output_dir)

    def run_fast_send_job(self, paths, output_dir, webhook_url):
        return quick_send.process_inputs_and_send(paths, output_dir, webhook_url)

    def run_cleanup_job(self, output_dir, dry_run):
        main.configure_paths(output_dir=output_dir)
        main.cleanup_duplicate_pdfs(output_dir, dry_run=dry_run)

    def run_clear_directory_job(self, target_dir):
        main.clear_directory_contents(target_dir)

    @Slot(str)
    def append_log(self, text):
        if not text:
            return
        self.log_box.insertPlainText(text)
        if self.log_panel_expanded:
            self.log_box.ensureCursorVisible()
        latest_line = ""
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            match = re.search(r"\[(\d+)/(\d+)\]", line)
            if match:
                self._set_live_progress(f"{match.group(1)}/{match.group(2)}")
            latest_line = line
        if latest_line:
            self._push_ticker_message(latest_line)

    @Slot(bool, str, object)
    def handle_worker_finished(self, success, message, result):
        self.current_action_name = ""
        self.current_progress_text = ""
        self.busy_timer.stop()
        self._apply_window_title()
        self.download_btn.setText("开始下载")
        self._apply_mode_ui()
        self._set_primary_busy(False)

        for widget in (self.download_btn, self.toggle_log_btn, self.more_button):
            widget.setDisabled(False)

        self._set_status_chip("完成" if success else "失败", "success" if success else "error")
        self._refresh_shortcuts()
        self._save_settings()

        if isinstance(result, dict) and result.get("mode") == "push":
            summary_message = (
                f"发票推送完成：共 {result['total']} 个输入，成功 {result['sent']} 个，失败 {result['failed']} 个。"
            )
            self._sync_status_labels(state="就绪", result=summary_message)
            self._push_ticker_message(summary_message)
            if result["sent"] > 0 and result["failed"] == 0:
                self._show_feedback(summary_message, "success", timeout_ms=5000)
            elif result["sent"] > 0:
                self._show_feedback(summary_message, "info", timeout_ms=6000)
            else:
                self._show_feedback(summary_message, "error", timeout_ms=7000)
            return

        if isinstance(result, dict) and result.get("mode") == "local":
            summary_message = (
                f"本地处理完成：共 {result['total']} 个输入，成功 {result['success']} 个，"
                f"跳过 {result['skipped']} 个，失败 {result['failed']} 个。"
            )
            self._sync_status_labels(state="就绪", result=summary_message)
            self._push_ticker_message(summary_message)
            if result["success"] > 0 and result["failed"] == 0:
                self._show_feedback(summary_message, "success", timeout_ms=5000)
            elif result["success"] > 0 or result["skipped"] > 0:
                self._show_feedback(summary_message, "info", timeout_ms=6000)
            else:
                self._show_feedback(summary_message, "error", timeout_ms=7000)
            return

        if isinstance(result, dict) and {"sent", "failed", "total"} <= set(result.keys()):
            summary_message = (
                f"快速发送完成：共 {result['total']} 个输入，成功 {result['sent']} 个，失败 {result['failed']} 个。"
            )
            self._sync_status_labels(state="就绪", result=summary_message)
            self._push_ticker_message(summary_message)
            if result["sent"] > 0 and result["failed"] == 0:
                self._show_feedback(summary_message, "success", timeout_ms=5000)
            elif result["sent"] > 0:
                self._show_feedback(summary_message, "info", timeout_ms=6000)
            else:
                self._show_feedback(summary_message, "error", timeout_ms=7000)
            return

        self._sync_status_labels(state="就绪", result=message)
        self._push_ticker_message(message)
        if success:
            self._show_feedback(message, "success", timeout_ms=4500)
            return
        self._show_feedback(f"{message} 如需查看详情，请点击“显示日志”。", "error", timeout_ms=8000)

    @Slot(bool, str)
    def on_worker_finished(self, success, message):
        self.current_action_name = ""
        self.busy_timer.stop()
        self.download_btn.setText("开始下载")
        self._apply_mode_ui()
        self._set_primary_busy(False)

        for widget in (self.download_btn, self.toggle_log_btn, self.more_button):
            widget.setDisabled(False)

        self._set_status_chip("完成" if success else "失败", "success" if success else "error")
        self._sync_status_labels(state="就绪", result=message)
        self._refresh_shortcuts()
        self._save_settings()
        self._push_ticker_message(message)

        if success:
            self._show_feedback(message, "success", timeout_ms=4500)
            return

        self._show_feedback(f"{message} 如需查看详情，请点击“显示日志”。", "error", timeout_ms=8000)

    @Slot()
    def _clear_worker_refs(self):
        self.worker = None
        self.worker_thread = None

    def _sync_status_labels(self, state="就绪", result="等待操作。"):
        self.inline_status_label.setText(f"当前状态：{state} | {result}")
        if self.current_mode == PROCESS_MODE_PUSH:
            self.inline_path_label.setText("")
        else:
            self.inline_path_label.setText(f"保存位置：{self.output_dir()}")

    def _dropped_process_paths(self, mime_data):
        if self.current_mode == PROCESS_MODE_PUSH:
            return quick_send.collect_supported_push_input_files(self._dropped_local_paths(mime_data))
        return quick_send.collect_supported_input_files(self._dropped_local_paths(mime_data))

    def _mode_hint_text(self):
        if self.current_mode == PROCESS_MODE_PUSH:
            return "选择待发送文件夹后可批量推送；也可以直接把 PDF、二维码图片或文件夹拖到窗口里发送到企业微信群。推送过程不保存到本地文件夹。"
        return "选择待处理文件夹和保存文件夹后可批量处理；二维码会自动下载发票，PDF 会按发票信息重命名后保存。"

    def _apply_mode_ui(self):
        mode = normalize_process_mode(self.current_mode)
        self.current_mode = mode

        if self.mode_combo is not None:
            index = self.mode_combo.findData(mode)
            if index >= 0 and self.mode_combo.currentIndex() != index:
                self.mode_combo.blockSignals(True)
                self.mode_combo.setCurrentIndex(index)
                self.mode_combo.blockSignals(False)

        if self.mode_hint_label is not None:
            self.mode_hint_label.setText(self._mode_hint_text())

        if hasattr(self, "input_path_field"):
            self.input_path_field.label.setText("待发送文件夹" if mode == PROCESS_MODE_PUSH else "待处理文件夹")
            if getattr(self.input_path_field, "drag_hint_button", None) is not None:
                self.input_path_field.drag_hint_button.setVisible(mode != PROCESS_MODE_PUSH)

        if hasattr(self, "output_path_field"):
            self.output_path_field.label.setText("发票保存文件夹")
            self.output_path_field.setVisible(mode != PROCESS_MODE_PUSH)

        if hasattr(self, "download_btn"):
            if mode == PROCESS_MODE_PUSH:
                self.download_btn.setText("开始推送到群")
                self.download_btn.setIcon(load_icon("send", self.style().standardIcon(QStyle.SP_ArrowForward)))
            else:
                self.download_btn.setText("开始本地下载")
                self.download_btn.setIcon(load_icon("download", self.style().standardIcon(QStyle.SP_ArrowDown)))

        self._apply_window_title()

        if hasattr(self, "open_input_action"):
            self.open_input_action.setText("打开待发送文件夹" if mode == PROCESS_MODE_PUSH else "打开待处理文件夹")

        if hasattr(self, "open_output_action"):
            self.open_output_action.setText("打开发票保存文件夹")
            self.open_output_action.setVisible(mode != PROCESS_MODE_PUSH)

    def _on_mode_changed(self, index):
        if self.mode_combo is None:
            return
        mode = normalize_process_mode(self.mode_combo.itemData(index))
        if mode == self.current_mode:
            return
        self.current_mode = mode
        self._hide_feedback()
        self._apply_mode_ui()
        self._sync_status_labels(result=f"已切换到{self._mode_label()}。")
        self._push_ticker_message(f"已切换到{self._mode_label()}。")
        self._save_settings()

    def _sync_status_labels(self, state="就绪", result="等待操作。"):
        self.inline_status_label.setText(f"当前模式：{self._mode_label()} | 当前状态：{state} | {result}")
        if self.current_mode == PROCESS_MODE_PUSH:
            self.inline_path_label.setText("推送模式：不会保存到本地文件夹")
        else:
            self.inline_path_label.setText(f"保存位置：{self.output_dir()}")

    def _dropped_process_paths(self, mime_data):
        if self.current_mode == PROCESS_MODE_PUSH:
            return quick_send.collect_supported_push_input_files(self._dropped_local_paths(mime_data))
        return quick_send.collect_supported_input_files(self._dropped_local_paths(mime_data))

    def _mode_hint_text(self):
        if self.current_mode == PROCESS_MODE_PUSH:
            return "选择待发送文件夹后可批量推送；也可以直接把 PDF、二维码图片或文件夹拖到窗口里发送到企业微信群。推送过程不保存到本地文件夹。"
        return "选择待处理文件夹和保存文件夹后可批量处理；二维码会自动下载发票，PDF 会按发票信息重命名后保存。"

    def _apply_mode_ui(self):
        mode = normalize_process_mode(self.current_mode)
        self.current_mode = mode

        if self.mode_combo is not None:
            index = self.mode_combo.findData(mode)
            if index >= 0 and self.mode_combo.currentIndex() != index:
                self.mode_combo.blockSignals(True)
                self.mode_combo.setCurrentIndex(index)
                self.mode_combo.blockSignals(False)

        if self.mode_hint_label is not None:
            self.mode_hint_label.setText(self._mode_hint_text())

        if hasattr(self, "input_path_field"):
            self.input_path_field.label.setText("待发送文件夹" if mode == PROCESS_MODE_PUSH else "待处理文件夹")

        if hasattr(self, "output_path_field"):
            self.output_path_field.label.setText("发票保存文件夹")
            self.output_path_field.setVisible(mode != PROCESS_MODE_PUSH)

        if hasattr(self, "push_mode_gap"):
            self.push_mode_gap.setVisible(mode == PROCESS_MODE_PUSH)

        if hasattr(self, "push_cutoff_field"):
            self.push_cutoff_field.setVisible(mode == PROCESS_MODE_PUSH)

        if hasattr(self, "download_btn"):
            if mode == PROCESS_MODE_PUSH:
                self.download_btn.setText("开始推送到群")
                self.download_btn.setIcon(load_icon("send", self.style().standardIcon(QStyle.SP_ArrowForward)))
            else:
                self.download_btn.setText("开始本地下载")
                self.download_btn.setIcon(load_icon("download", self.style().standardIcon(QStyle.SP_ArrowDown)))

        if self.window_title_label is not None:
            self.window_title_label.setText("发票处理工具")
        self.setWindowTitle("发票处理工具")

        if hasattr(self, "open_input_action"):
            self.open_input_action.setText("打开待发送文件夹" if mode == PROCESS_MODE_PUSH else "打开待处理文件夹")

        if hasattr(self, "open_output_action"):
            self.open_output_action.setText("打开发票保存文件夹")
            self.open_output_action.setVisible(mode != PROCESS_MODE_PUSH)

    def _sync_status_labels(self, state="就绪", result="等待操作。"):
        self.inline_status_label.setText(f"当前模式：{self._mode_label()} | 当前状态：{state} | {result}")
        if self.current_mode == PROCESS_MODE_PUSH:
            self.inline_path_label.setText("推送模式：不会保存到本地文件夹")
        else:
            self.inline_path_label.setText(f"保存位置：{self.output_dir()}")

    def run_fast_send_job(self, paths, output_dir, webhook_url, send_interval_seconds=None):
        if send_interval_seconds is None:
            send_interval_seconds = self._wecom_send_interval_seconds()
        return quick_send.process_inputs_and_send(
            paths,
            output_dir,
            webhook_url,
            send_interval_seconds=send_interval_seconds,
            batch_cutoff_date=self._current_push_batch_cutoff_date(),
            batch_summary_template=self._push_summary_template(),
        )

    def save_wecom_settings(self):
        webhook_url = self._wecom_webhook()
        webhook_note = self._wecom_webhook_note()
        send_interval_seconds = self._wecom_send_interval_seconds()

        if self.wecom_webhook_edit is not None:
            webhook_url = self.wecom_webhook_edit.text().strip()
        if self.wecom_webhook_note_edit is not None:
            webhook_note = self.wecom_webhook_note_edit.text().strip()
        if self.wecom_send_interval_spin is not None:
            send_interval_seconds = self._normalize_wecom_send_interval(self.wecom_send_interval_spin.value())

        if not webhook_url:
            self._show_feedback("请先填写企业微信 Webhook 地址。", "error", timeout_ms=3000)
            return

        self._remember_wecom_webhook(webhook_url, webhook_note or "未命名机器人")
        self.wecom_send_interval_seconds = send_interval_seconds
        self._commit_push_message_settings(save_settings=False, show_feedback=False)
        self._refresh_wecom_webhook_combo()
        self._save_settings()
        self._show_feedback(
            f"企业微信机器人已保存：{self._wecom_webhook_note() or '未命名机器人'}，"
            f"发送间隔 {self._wecom_send_interval_seconds():.1f} 秒。"
            f"{self._wecom_send_interval_notice(self._wecom_send_interval_seconds())}",
            "success",
            timeout_ms=2500,
        )

    def save_wecom_settings_and_close(self):
        webhook_url = self._wecom_webhook()
        webhook_note = self._wecom_webhook_note()
        send_interval_seconds = self._wecom_send_interval_seconds()

        if self.wecom_webhook_edit is not None:
            webhook_url = self.wecom_webhook_edit.text().strip()
        if self.wecom_webhook_note_edit is not None:
            webhook_note = self.wecom_webhook_note_edit.text().strip()
        if self.wecom_send_interval_spin is not None:
            send_interval_seconds = self._normalize_wecom_send_interval(self.wecom_send_interval_spin.value())

        if not webhook_url:
            if self.wecom_status_label is not None:
                self.wecom_status_label.setText("请先填写企业微信 Webhook 地址。")
            self._show_feedback("请先填写企业微信 Webhook 地址。", "error", timeout_ms=3000)
            return

        self._remember_wecom_webhook(webhook_url, webhook_note or "未命名机器人")
        self.wecom_send_interval_seconds = send_interval_seconds
        self._commit_push_message_settings(save_settings=False, show_feedback=False)
        self._refresh_wecom_webhook_combo()
        self._save_settings()
        if self.wecom_status_label is not None:
            self.wecom_status_label.setText(
                f"已保存：{self._wecom_webhook_note() or '未命名机器人'} | "
                f"发送间隔 {self._wecom_send_interval_seconds():.1f} 秒 | "
                f"{self._wecom_send_interval_notice(self._wecom_send_interval_seconds())}"
            )
        self._show_feedback(
            f"已保存企业微信机器人：{self._wecom_webhook_note() or '未命名机器人'}，"
            f"发送间隔 {self._wecom_send_interval_seconds():.1f} 秒。"
            f"{self._wecom_send_interval_notice(self._wecom_send_interval_seconds())}",
            "success",
            timeout_ms=2500,
        )
        self._push_ticker_message(
            f"已保存企业微信机器人：{self._wecom_webhook_note() or '未命名机器人'}，"
            f"发送间隔 {self._wecom_send_interval_seconds():.1f} 秒。"
            f"{self._wecom_send_interval_notice(self._wecom_send_interval_seconds())}"
        )
        if self.wecom_window is not None:
            self.wecom_window.hide()
        self.raise_()
        self.activateWindow()

    def show_wecom_window(self):
        if self.wecom_window is None:
            window = QWidget(None, Qt.Window)
            window.setWindowTitle("企业微信 Webhook 设置")
            window.setWindowIcon(self.window_icon)
            window.resize(680, 520)

            layout = QVBoxLayout(window)
            layout.setContentsMargins(12, 12, 12, 12)
            layout.setSpacing(8)

            title = QLabel("企业微信 Webhook")
            title.setObjectName("SectionTitle")
            layout.addWidget(title)

            hint = QLabel(
                "把群机器人的 Webhook 地址填在这里。"
                "发送间隔支持单独配置；如果遇到限流，程序会自动退避重试，"
                "本轮结束后还会自动补发失败的 PDF。"
            )
            hint.setObjectName("HelpText")
            hint.setWordWrap(True)
            layout.addWidget(hint)

            saved_combo = QComboBox()
            saved_combo.setObjectName("PathInput")
            saved_combo.currentIndexChanged.connect(self._on_wecom_webhook_selected)
            layout.addWidget(saved_combo)

            webhook_edit = QLineEdit()
            webhook_edit.setObjectName("PathInput")
            webhook_edit.setPlaceholderText("https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=...")
            webhook_edit.setText(self._wecom_webhook())
            layout.addWidget(webhook_edit)

            note_edit = QLineEdit()
            note_edit.setObjectName("PathInput")
            note_edit.setPlaceholderText("机器人备注，例如：财务群")
            note_edit.setText(self._wecom_webhook_note())
            layout.addWidget(note_edit)

            interval_row = QHBoxLayout()
            interval_row.setContentsMargins(0, 0, 0, 0)
            interval_row.setSpacing(8)

            interval_label = QLabel("发送间隔（秒/份）")
            interval_label.setObjectName("HelpText")
            interval_row.addWidget(interval_label)

            interval_spin = QDoubleSpinBox()
            interval_spin.setObjectName("PathInput")
            interval_spin.setDecimals(1)
            interval_spin.setRange(0.0, 60.0)
            interval_spin.setSingleStep(0.5)
            interval_spin.setValue(self._wecom_send_interval_seconds())
            interval_spin.setSuffix(" s")
            interval_spin.setMinimumHeight(36)
            interval_row.addWidget(interval_spin)
            interval_row.addStretch(1)
            layout.addLayout(interval_row)

            interval_notice = QLabel("建议发送间隔不低于 2.0 秒，过低容易触发微信限流。")
            interval_notice.setObjectName("HelpText")
            interval_notice.setWordWrap(True)
            layout.addWidget(interval_notice)

            summary_title = QLabel("推送完成提醒文案")
            summary_title.setObjectName("FieldLabel")
            layout.addWidget(summary_title)

            summary_hint = QLabel(
                "支持变量 {count} 和 {cutoff_date}，保存后会自动替换成张数和截止时间。"
            )
            summary_hint.setObjectName("HelpText")
            summary_hint.setWordWrap(True)
            layout.addWidget(summary_hint)

            summary_template_edit = QPlainTextEdit()
            summary_template_edit.setObjectName("PushSummaryTemplateInput")
            summary_template_edit.setMinimumHeight(140)
            summary_template_edit.setPlainText(self._push_summary_template())
            layout.addWidget(summary_template_edit)

            status_label = QLabel("")
            status_label.setObjectName("HelpText")
            status_label.setWordWrap(True)
            layout.addWidget(status_label)

            buttons = QHBoxLayout()
            buttons.setContentsMargins(0, 0, 0, 0)
            buttons.addStretch(1)

            save_btn = QPushButton("保存")
            save_btn.setProperty("role", "subtle")
            save_btn.setObjectName("CompactButton")
            save_btn.setFixedWidth(82)
            save_btn.clicked.connect(self.save_wecom_settings_and_close)
            buttons.addWidget(save_btn)

            layout.addLayout(buttons)

            self.wecom_window = window
            self.wecom_webhook_combo = saved_combo
            self.wecom_webhook_edit = webhook_edit
            self.wecom_webhook_note_edit = note_edit
            self.wecom_send_interval_spin = interval_spin
            self.push_summary_template_edit = summary_template_edit
            self.wecom_status_label = status_label
        else:
            self._refresh_wecom_webhook_combo()
            self.wecom_webhook_edit.setText(self._wecom_webhook())
            self.wecom_webhook_note_edit.setText(self._wecom_webhook_note())
            if self.wecom_send_interval_spin is not None:
                self.wecom_send_interval_spin.setValue(self._wecom_send_interval_seconds())
            if self.push_summary_template_edit is not None:
                self.push_summary_template_edit.setPlainText(self._push_summary_template())
            if self.wecom_status_label is not None:
                self.wecom_status_label.setText("")

        self._refresh_wecom_webhook_combo()
        self.wecom_window.show()
        self.wecom_window.raise_()
        self.wecom_window.activateWindow()

    @Slot(bool, str, object)
    def handle_worker_finished(self, success, message, result):
        self.current_action_name = ""
        self.busy_timer.stop()
        self.download_btn.setText("开始下载")
        self._apply_mode_ui()
        self._set_primary_busy(False)

        for widget in (self.download_btn, self.toggle_log_btn, self.more_button):
            widget.setDisabled(False)

        self._set_status_chip("完成" if success else "失败", "success" if success else "error")
        self._refresh_shortcuts()
        self._save_settings()

        if isinstance(result, dict) and result.get("mode") == "push":
            summary_message = (
                f"发票推送完成：共 {result['total']} 个输入，"
                f"成功 {result['sent']} 个，失败 {result['failed']} 个。"
            )
            extras = []
            if result.get("resend_queued"):
                extras.append(
                    f"失败 PDF 补发 {result.get('resend_sent', 0)}/{result.get('resend_queued', 0)}"
                )
            if result.get("rate_limit_retries"):
                extras.append(f"限流重试 {result['rate_limit_retries']} 次")
            if extras:
                summary_message = f"{summary_message} {'，'.join(extras)}。"

            self._sync_status_labels(state="就绪", result=summary_message)
            self._push_ticker_message(summary_message)
            if result["sent"] > 0 and result["failed"] == 0:
                self._show_feedback(summary_message, "success", timeout_ms=5000)
            elif result["sent"] > 0:
                self._show_feedback(summary_message, "info", timeout_ms=6500)
            else:
                self._show_feedback(summary_message, "error", timeout_ms=7000)
            return

        if isinstance(result, dict) and result.get("mode") == "local":
            summary_message = (
                f"本地处理完成：共 {result['total']} 个输入，成功 {result['success']} 个，"
                f"跳过 {result['skipped']} 个，失败 {result['failed']} 个。"
            )
            self._sync_status_labels(state="就绪", result=summary_message)
            self._push_ticker_message(summary_message)
            if result["success"] > 0 and result["failed"] == 0:
                self._show_feedback(summary_message, "success", timeout_ms=5000)
            elif result["success"] > 0 or result["skipped"] > 0:
                self._show_feedback(summary_message, "info", timeout_ms=6000)
            else:
                self._show_feedback(summary_message, "error", timeout_ms=7000)
            return

        self._sync_status_labels(state="就绪", result=message)
        self._push_ticker_message(message)
        if success:
            self._show_feedback(message, "success", timeout_ms=4500)
            return
        self._show_feedback(f"{message} 如需查看详情，请点击“显示日志”。", "error", timeout_ms=8000)

    def _schedule_failed_pdf_retry(self, folder_path, webhook_url, send_interval_seconds, delay_ms=60000):
        folder_path = os.path.abspath(folder_path)
        self.pending_failed_pdf_retry = {
            "folder_path": folder_path,
            "webhook_url": webhook_url,
            "send_interval_seconds": send_interval_seconds,
        }
        if not hasattr(self, "failed_pdf_retry_timer") or self.failed_pdf_retry_timer is None:
            self.failed_pdf_retry_timer = QTimer(self)
            self.failed_pdf_retry_timer.setSingleShot(True)
            self.failed_pdf_retry_timer.timeout.connect(self._run_scheduled_failed_pdf_retry)
        self.failed_pdf_retry_timer.start(delay_ms)
        seconds = max(1, int(delay_ms / 1000))
        self._push_ticker_message(f"已安排 {seconds} 秒后重新发送失败 PDF。")
        self._show_feedback(f"已安排 {seconds} 秒后重新发送失败 PDF。", "info", timeout_ms=3500)

    def _run_scheduled_failed_pdf_retry(self):
        retry_info = getattr(self, "pending_failed_pdf_retry", None)
        if not retry_info:
            return
        if self.worker_thread is not None:
            self._schedule_failed_pdf_retry(
                retry_info["folder_path"],
                retry_info["webhook_url"],
                retry_info["send_interval_seconds"],
                delay_ms=10000,
            )
            return
        self.pending_failed_pdf_retry = None
        self.start_worker(
            "重新发送失败 PDF",
            lambda: self.run_fast_send_job(
                [retry_info["folder_path"]],
                self.output_dir(),
                retry_info["webhook_url"],
                retry_info["send_interval_seconds"],
            ),
        )

    def _prompt_failed_push_follow_up(self, result):
        manual_retry_dir = os.path.abspath(result.get("manual_retry_dir") or "")
        if not manual_retry_dir or not os.path.isdir(manual_retry_dir):
            return

        message_box = QMessageBox(self)
        message_box.setWindowTitle("部分推送仍失败")
        message_box.setIcon(QMessageBox.Warning)
        message_box.setText(
            f"还有 {result.get('resend_failed', 0)} 个 PDF 在自动重试和补发后仍未发送成功。"
        )
        message_box.setInformativeText(
            "可以选择 1 分钟后自动再发一次，或者先打开文件夹人工发送。"
        )
        retry_button = message_box.addButton("一分钟后重新发送", QMessageBox.AcceptRole)
        open_button = message_box.addButton("打开文件夹人工发送", QMessageBox.ActionRole)
        message_box.exec()

        clicked = message_box.clickedButton()
        if clicked == retry_button:
            self._schedule_failed_pdf_retry(
                manual_retry_dir,
                self._wecom_webhook(),
                result.get("send_interval_seconds", self._wecom_send_interval_seconds()),
                delay_ms=60000,
            )
            return
        if clicked == open_button:
            self.open_folder(manual_retry_dir)

    @Slot(bool, str, object)
    def handle_worker_finished(self, success, message, result):
        self.current_action_name = ""
        self.busy_timer.stop()
        self.download_btn.setText("开始下载")
        self._apply_mode_ui()
        self._set_primary_busy(False)

        for widget in (self.download_btn, self.toggle_log_btn, self.more_button):
            widget.setDisabled(False)

        self._set_status_chip("完成" if success else "失败", "success" if success else "error")
        self._refresh_shortcuts()
        self._save_settings()

        if isinstance(result, dict) and result.get("mode") == "push":
            summary_message = (
                f"发票推送完成：共 {result['total']} 个输入，成功 {result['sent']} 个，失败 {result['failed']} 个。"
            )
            extras = []
            if result.get("resend_queued"):
                extras.append(f"补发 {result.get('resend_sent', 0)}/{result.get('resend_queued', 0)}")
            if result.get("rate_limit_retries"):
                extras.append(f"限流重试 {result['rate_limit_retries']} 次")
            if result.get("manual_retry_files"):
                extras.append(f"落地待人工处理 {len(result['manual_retry_files'])} 个")
            if result.get("batch_zip_sent") and result.get("batch_zip_name"):
                extras.append(f"已推送汇总包 {result['batch_zip_name']}")
            if result.get("summary_text_sent"):
                extras.append("已发送完成提醒")
            if result.get("post_push_failed"):
                extras.append(f"汇总推送失败 {result['post_push_failed']} 项")
            if extras:
                summary_message = f"{summary_message} {'，'.join(extras)}。"

            self._sync_status_labels(state="就绪", result=summary_message)
            self._push_ticker_message(summary_message)
            if result["sent"] > 0 and result["failed"] == 0:
                self._show_feedback(summary_message, "success", timeout_ms=5000)
            elif result["sent"] > 0:
                self._show_feedback(summary_message, "info", timeout_ms=6500)
            else:
                self._show_feedback(summary_message, "error", timeout_ms=7000)

            if result.get("resend_failed", 0) > 0 and result.get("manual_retry_dir"):
                self._prompt_failed_push_follow_up(result)
            return

        if isinstance(result, dict) and result.get("mode") == "local":
            summary_message = (
                f"本地处理完成：共 {result['total']} 个输入，成功 {result['success']} 个，"
                f"跳过 {result['skipped']} 个，失败 {result['failed']} 个。"
            )
            self._sync_status_labels(state="就绪", result=summary_message)
            self._push_ticker_message(summary_message)
            if result["success"] > 0 and result["failed"] == 0:
                self._show_feedback(summary_message, "success", timeout_ms=5000)
            elif result["success"] > 0 or result["skipped"] > 0:
                self._show_feedback(summary_message, "info", timeout_ms=6000)
            else:
                self._show_feedback(summary_message, "error", timeout_ms=7000)
            return

        self._sync_status_labels(state="就绪", result=message)
        self._push_ticker_message(message)
        if success:
            self._show_feedback(message, "success", timeout_ms=4500)
            return
        self._show_feedback(f"{message} 如需查看详情，请点击“显示日志”。", "error", timeout_ms=8000)

    def run_first_launch_setup_if_needed(self):
        return None

    def closeEvent(self, event):
        self._save_settings()
        super().closeEvent(event)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        if hasattr(self, "drop_overlay"):
            self.drop_overlay.setGeometry(self.centralWidget().rect())
        self._position_feedback_banner()

    def dragEnterEvent(self, event):
        if hasattr(self, "drop_overlay") and self.drop_overlay.isVisible():
            super().dragEnterEvent(event)
            return
        if self._dropped_process_paths(event.mimeData()):
            event.acceptProposedAction()
            return
        super().dragEnterEvent(event)

    def dragMoveEvent(self, event):
        if hasattr(self, "drop_overlay") and self.drop_overlay.isVisible():
            super().dragMoveEvent(event)
            return
        if self._dropped_process_paths(event.mimeData()):
            event.acceptProposedAction()
            return
        super().dragMoveEvent(event)

    def dropEvent(self, event):
        if hasattr(self, "drop_overlay") and self.drop_overlay.isVisible():
            super().dropEvent(event)
            return

        local_paths = self._dropped_local_paths(event.mimeData())
        if self.current_mode == PROCESS_MODE_PUSH:
            file_paths = quick_send.collect_supported_push_input_files(local_paths)
        else:
            file_paths = quick_send.collect_supported_input_files(local_paths)
        if file_paths:
            event.acceptProposedAction()
            self.start_process_from_paths(local_paths)
            return
        super().dropEvent(event)

    def keyPressEvent(self, event):
        if event.key() == Qt.Key_Escape and hasattr(self, "drop_overlay") and self.drop_overlay.isVisible():
            self.exit_drop_mode()
            event.accept()
            return
        super().keyPressEvent(event)

    def _apply_mode_ui(self):
        mode = normalize_process_mode(self.current_mode)
        self.current_mode = mode

        if self.mode_combo is not None:
            index = self.mode_combo.findData(mode)
            if index >= 0 and self.mode_combo.currentIndex() != index:
                self.mode_combo.blockSignals(True)
                self.mode_combo.setCurrentIndex(index)
                self.mode_combo.blockSignals(False)

        if self.mode_hint_label is not None:
            self.mode_hint_label.setText(self._mode_hint_text())

        if hasattr(self, "input_path_field"):
            self.input_path_field.label.setText("待发送文件夹" if mode == PROCESS_MODE_PUSH else "待处理文件夹")
            if getattr(self.input_path_field, "drag_hint_button", None) is not None:
                self.input_path_field.drag_hint_button.setVisible(mode != PROCESS_MODE_PUSH)

        if hasattr(self, "output_path_field"):
            self.output_path_field.label.setText("发票保存文件夹")
            self.output_path_field.setVisible(mode != PROCESS_MODE_PUSH)

        if hasattr(self, "push_mode_gap"):
            self.push_mode_gap.setVisible(mode == PROCESS_MODE_PUSH)

        if hasattr(self, "push_cutoff_field"):
            self.push_cutoff_field.setVisible(mode == PROCESS_MODE_PUSH)

        if hasattr(self, "download_btn") and not self.current_action_name:
            if mode == PROCESS_MODE_PUSH:
                self.download_btn.setText("开始推送到群")
                self.download_btn.setIcon(load_icon("send", self.style().standardIcon(QStyle.SP_ArrowForward)))
            else:
                self.download_btn.setText("开始本地下载")
                self.download_btn.setIcon(load_icon("download", self.style().standardIcon(QStyle.SP_ArrowDown)))

        self._apply_window_title()

        if hasattr(self, "open_input_action"):
            self.open_input_action.setText("打开待发送文件夹" if mode == PROCESS_MODE_PUSH else "打开待处理文件夹")

        if hasattr(self, "open_output_action"):
            self.open_output_action.setText("打开发票保存文件夹")
            self.open_output_action.setVisible(mode != PROCESS_MODE_PUSH)

    def _sync_status_labels(self, state="就绪", result="等待操作。"):
        self.inline_status_label.setText(f"当前状态：{state} | {result}")
        if self.current_mode == PROCESS_MODE_PUSH:
            progress = (self.current_progress_text or "").strip()
            self.inline_path_label.setText(progress if progress else "")
        else:
            self.inline_path_label.setText(f"保存位置：{self.output_dir()}")

    def append_log(self, text):
        if not text:
            return
        self.log_box.insertPlainText(text)
        if self.log_panel_expanded:
            self.log_box.ensureCursorVisible()
        latest_line = ""
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            match = re.search(r"\[(\d+)/(\d+)\]", line)
            if match:
                self._set_live_progress(f"{match.group(1)}/{match.group(2)}")
            latest_line = line
        if latest_line:
            self._push_ticker_message(latest_line)

    def save_wecom_settings(self):
        webhook_url = self._wecom_webhook()
        webhook_note = self._wecom_webhook_note()
        send_interval_seconds = self._wecom_send_interval_seconds()

        if self.wecom_webhook_edit is not None:
            webhook_url = self.wecom_webhook_edit.text().strip()
        if self.wecom_webhook_note_edit is not None:
            webhook_note = self.wecom_webhook_note_edit.text().strip()
        if self.wecom_send_interval_spin is not None:
            send_interval_seconds = self._normalize_wecom_send_interval(self.wecom_send_interval_spin.value())

        if not webhook_url:
            self._show_feedback("请先填写企业微信 Webhook 地址。", "error", timeout_ms=3000)
            return

        self._remember_wecom_webhook(webhook_url, webhook_note or "未命名机器人")
        self.wecom_send_interval_seconds = send_interval_seconds
        self._commit_push_message_settings(save_settings=False, show_feedback=False)
        self._refresh_wecom_webhook_combo()
        self._save_settings()
        self._show_feedback(
            f"企业微信机器人已保存：{self._wecom_webhook_note() or '未命名机器人'}，"
            f"发送间隔 {self._wecom_send_interval_seconds():.1f} 秒。"
            f"{self._wecom_send_interval_notice(self._wecom_send_interval_seconds())}",
            "success",
            timeout_ms=3000,
        )

    def save_wecom_settings_and_close(self):
        webhook_url = self._wecom_webhook()
        webhook_note = self._wecom_webhook_note()
        send_interval_seconds = self._wecom_send_interval_seconds()

        if self.wecom_webhook_edit is not None:
            webhook_url = self.wecom_webhook_edit.text().strip()
        if self.wecom_webhook_note_edit is not None:
            webhook_note = self.wecom_webhook_note_edit.text().strip()
        if self.wecom_send_interval_spin is not None:
            send_interval_seconds = self._normalize_wecom_send_interval(self.wecom_send_interval_spin.value())

        if not webhook_url:
            if self.wecom_status_label is not None:
                self.wecom_status_label.setText("请先填写企业微信 Webhook 地址。")
            self._show_feedback("请先填写企业微信 Webhook 地址。", "error", timeout_ms=3000)
            return

        self._remember_wecom_webhook(webhook_url, webhook_note or "未命名机器人")
        self.wecom_send_interval_seconds = send_interval_seconds
        self._commit_push_message_settings(save_settings=False, show_feedback=False)
        self._refresh_wecom_webhook_combo()
        self._save_settings()

        notice = self._wecom_send_interval_notice(self._wecom_send_interval_seconds())
        if self.wecom_status_label is not None:
            self.wecom_status_label.setText(
                f"已保存：{self._wecom_webhook_note() or '未命名机器人'} | "
                f"发送间隔 {self._wecom_send_interval_seconds():.1f} 秒 | {notice}"
            )
        self._show_feedback(
            f"已保存企业微信机器人：{self._wecom_webhook_note() or '未命名机器人'}，"
            f"发送间隔 {self._wecom_send_interval_seconds():.1f} 秒。{notice}",
            "success",
            timeout_ms=3000,
        )
        self._push_ticker_message(
            f"已保存企业微信机器人：{self._wecom_webhook_note() or '未命名机器人'}，"
            f"发送间隔 {self._wecom_send_interval_seconds():.1f} 秒。{notice}"
        )
        if self.wecom_window is not None:
            self.wecom_window.hide()
        self.raise_()
        self.activateWindow()

    def handle_worker_finished(self, success, message, result):
        self.current_action_name = ""
        self.current_progress_text = ""
        self.busy_timer.stop()
        self._apply_window_title()
        self.download_btn.setText("开始下载")
        self._apply_mode_ui()
        self._set_primary_busy(False)

        for widget in (self.download_btn, self.toggle_log_btn, self.more_button):
            widget.setDisabled(False)

        self._set_status_chip("完成" if success else "失败", "success" if success else "error")
        self._refresh_shortcuts()
        self._save_settings()

        if isinstance(result, dict) and result.get("mode") == "push":
            summary_message = (
                f"发票推送完成：共 {result['total']} 个输入，成功 {result['sent']} 个，失败 {result['failed']} 个。"
            )
            extras = []
            if result.get("resend_queued"):
                extras.append(f"补发 {result.get('resend_sent', 0)}/{result.get('resend_queued', 0)}")
            if result.get("rate_limit_retries"):
                extras.append(f"限流重试 {result['rate_limit_retries']} 次")
            if result.get("manual_retry_files"):
                extras.append(f"落地待人工处理 {len(result['manual_retry_files'])} 个")
            if result.get("batch_zip_sent") and result.get("batch_zip_name"):
                extras.append(f"已推送汇总包 {result['batch_zip_name']}")
            if result.get("summary_text_sent"):
                extras.append("已发送完成提醒")
            if result.get("post_push_failed"):
                extras.append(f"汇总推送失败 {result['post_push_failed']} 项")
            if extras:
                summary_message = f"{summary_message} {'；'.join(extras)}。"

            self._sync_status_labels(state="就绪", result=summary_message)
            self._push_ticker_message(summary_message)
            if result["sent"] > 0 and result["failed"] == 0:
                self._show_feedback(summary_message, "success", timeout_ms=5000)
            elif result["sent"] > 0:
                self._show_feedback(summary_message, "info", timeout_ms=6500)
            else:
                self._show_feedback(summary_message, "error", timeout_ms=7000)

            if result.get("resend_failed", 0) > 0 and result.get("manual_retry_dir"):
                self._prompt_failed_push_follow_up(result)
            return

        if isinstance(result, dict) and result.get("mode") == "local":
            summary_message = (
                f"本地处理完成：共 {result['total']} 个输入，成功 {result['success']} 个，"
                f"跳过 {result['skipped']} 个，失败 {result['failed']} 个。"
            )
            self._sync_status_labels(state="就绪", result=summary_message)
            self._push_ticker_message(summary_message)
            if result["success"] > 0 and result["failed"] == 0:
                self._show_feedback(summary_message, "success", timeout_ms=5000)
            elif result["success"] > 0 or result["skipped"] > 0:
                self._show_feedback(summary_message, "info", timeout_ms=6000)
            else:
                self._show_feedback(summary_message, "error", timeout_ms=7000)
            return

        self._sync_status_labels(state="就绪", result=message)
        self._push_ticker_message(message)
        if success:
            self._show_feedback(message, "success", timeout_ms=4500)
            return
        self._show_feedback(f"{message} 如需查看详情，请点击“显示日志”。", "error", timeout_ms=8000)


def run_app():
    main.enable_utf8_console()
    set_windows_app_id()
    app = QApplication(sys.argv)
    coordinator = SingleInstanceCoordinator(app)
    coordinator.acquire()
    app.aboutToQuit.connect(coordinator.stop)
    app._single_instance_coordinator = coordinator
    app.setWindowIcon(load_app_icon())
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    try:
        sys.exit(run_app())
    except Exception:
        with open(ERROR_LOG_PATH, "a", encoding="utf-8") as file_obj:
            file_obj.write(traceback.format_exc())
            file_obj.write("\n")
        raise
