import argparse
import contextlib
import ctypes
import hashlib
import json
import os
import re
import shutil
import sys
import time
from urllib.parse import parse_qs, urlparse

import pdfplumber
import requests
from PIL import Image
from pyzbar.pyzbar import decode

INPUT_DIR = "./qrcode_images"
OUTPUT_DIR = "./invoice_pdfs"
INDEX_FILE = os.path.join(OUTPUT_DIR, "processed_index.json")
FAILED_LOG_FILE = os.path.join(OUTPUT_DIR, "failed.txt")
REQUEST_TIMEOUT_SECONDS = 30
MAX_DOWNLOAD_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1
EXPORT_URL = "https://dppt.jiangsu.chinatax.gov.cn:8443/kpfw/fpjfzz/v1/exportDzfpwjEwm"
DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/135.0.0.0 Safari/537.36"
    ),
    "Accept": "application/pdf,application/json,text/plain,*/*",
}
TIMESTAMPED_PDF_RE = re.compile(r"^(?P<base>.+)_\d{10}\.pdf$", re.IGNORECASE)


def enable_utf8_console():
    if os.name != "nt":
        return

    with contextlib.suppress(Exception):
        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleCP(65001)
        kernel32.SetConsoleOutputCP(65001)

    for stream_name in ("stdin", "stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if hasattr(stream, "reconfigure"):
            with contextlib.suppress(Exception):
                stream.reconfigure(encoding="utf-8", errors="replace")


def configure_paths(input_dir=None, output_dir=None):
    global INPUT_DIR, OUTPUT_DIR, INDEX_FILE, FAILED_LOG_FILE

    if input_dir:
        INPUT_DIR = input_dir
    if output_dir:
        OUTPUT_DIR = output_dir

    INDEX_FILE = os.path.join(OUTPUT_DIR, "processed_index.json")
    FAILED_LOG_FILE = os.path.join(OUTPUT_DIR, "failed.txt")


def extract_invoice_metadata(pdf_path):
    buyer = None
    amount = None
    invoice_number = None

    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages[:2]:
                try:
                    text += "\n" + (page.extract_text() or "")
                except Exception:
                    continue

        if not text:
            return None, None, None

        buyer_patterns = [
            r"购买方\s*名称[:：]?\s*([^\n]+)",
            r"购买方[:：]?\s*([^\n]+)",
            r"收款方[:：]?\s*([^\n]+)",
            r"名称[:：]?\s*([^\n]+)",
        ]
        for pattern in buyer_patterns:
            match = re.search(pattern, text)
            if not match:
                continue

            candidate = match.group(1).strip()
            candidate = re.split(r"\s{2,}|[,，；;]\s*", candidate)[0]
            candidate = re.split(
                r"销售方|销售\s*名称|销售方[:：]|销\s*名称[:：]|销\s*方|销[:：]",
                candidate,
            )[0]
            candidate = re.sub(r'[\\/:*?"<>|]', "", candidate).strip()
            if candidate:
                buyer = candidate
                break

        compact_text = re.sub(r"\s+", "", text)
        amount_patterns = [
            r"价税合计.*?[（(]小写[)）]\s*[￥¥]?([\d,]+(?:\.\d+)?)",
            r"[（(]小写[)）]\s*[￥¥]?([\d,]+(?:\.\d+)?)",
            r"价税合计金额[:：]?[￥¥]?([\d,]+(?:\.\d+)?)",
        ]
        for pattern in amount_patterns:
            match = re.search(pattern, compact_text)
            if not match:
                continue

            raw_amount = re.sub(r"[￥¥,\s]", "", match.group(1))
            try:
                amount = f"{float(raw_amount):.2f}"
            except ValueError:
                amount = raw_amount
            break

        invoice_number_patterns = [
            r"(?:鍙戠エ鍙风爜|绁ㄦ嵁鍙风爜|Invoice\s*No\.?)[:锛歖?\s*([0-9]{8,20})",
            r"鍙风爜[:锛歖?\s*([0-9]{8,20})",
        ]
        for pattern in invoice_number_patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                invoice_number = match.group(1).strip()
                break

    except Exception as exc:
        print(f"解析 PDF 文本失败: {exc}")

    return buyer, amount, invoice_number


def get_invoice_info(pdf_path):
    buyer, amount, _invoice_number = extract_invoice_metadata(pdf_path)
    return buyer, amount


def sanitize_filename(name):
    return re.sub(r'[\\/:*?"<>|]', "", name).strip()


def extract_cs_from_url(url):
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    if parsed.path.endswith("/qrcode") and query.get("cs"):
        return query["cs"][0]
    if "/v/" in parsed.path:
        cs_value = parsed.path.rsplit("/v/", 1)[-1].strip("/")
        if cs_value:
            return cs_value
    return None


def resolve_entry_url(session, url):
    cs_value = extract_cs_from_url(url)
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    if parsed.path.endswith("/qrcode") and query.get("cs"):
        return cs_value, url

    response = session.get(url, allow_redirects=True, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    final_url = response.url
    final_query = parse_qs(urlparse(final_url).query)
    final_cs_value = final_query.get("cs", [cs_value])[0]
    if not final_cs_value:
        raise ValueError(f"无法从二维码链接中提取 cs 参数: {final_url}")
    return final_cs_value, final_url


def build_export_params(cs_value):
    parts = cs_value.split("_", 2)
    if len(parts) != 3:
        raise ValueError(f"二维码参数格式不符合预期: {cs_value}")

    invoice_number = parts[1]
    tail = parts[2]
    if len(tail) < 17:
        raise ValueError(f"二维码尾串长度不足，无法拆分日期和校验码: {cs_value}")

    issue_time = tail[:14]
    suffix = tail[14:]
    if "XH" in suffix:
        check_code = suffix.split("XH", 1)[1]
    else:
        check_code = tail[-4:]

    if not check_code:
        raise ValueError(f"二维码尾串中缺少校验码: {cs_value}")

    return {
        "Wjgs": "PDF",
        "Fphm": invoice_number,
        "Kprq": issue_time,
        "Jym": check_code,
        "Czsj": str(int(time.time() * 1000)),
    }


def save_response_pdf(response):
    content_type = (response.headers.get("content-type") or "").lower()
    content = response.content or b""
    looks_like_pdf = "application/pdf" in content_type or content.startswith(b"%PDF")

    if response.status_code != 200:
        raise ValueError(f"下载接口返回 {response.status_code}: {response.text[:200]}")
    if not content:
        raise ValueError("下载接口返回空内容，请检查二维码参数拆分是否正确。")
    if not looks_like_pdf:
        raise ValueError(
            f"下载接口未返回 PDF，响应类型: {content_type or 'unknown'}，内容片段: {response.text[:200]}"
        )

    temp_path = os.path.join(OUTPUT_DIR, f"temp_{int(time.time() * 1000)}.pdf")
    with open(temp_path, "wb") as file_obj:
        file_obj.write(content)
    return temp_path


def download_invoice_pdf(url):
    last_error = None

    for attempt in range(1, MAX_DOWNLOAD_RETRIES + 1):
        try:
            session = requests.Session()
            session.headers.update(DEFAULT_HEADERS)

            cs_value, referer_url = resolve_entry_url(session, url)
            params = build_export_params(cs_value)
            response = session.get(
                EXPORT_URL,
                params=params,
                headers={"Referer": referer_url},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            temp_path = save_response_pdf(response)
            return temp_path, cs_value, params["Fphm"]
        except Exception as exc:
            last_error = exc
            if attempt < MAX_DOWNLOAD_RETRIES:
                print(f"下载重试 {attempt}/{MAX_DOWNLOAD_RETRIES - 1}: {exc}")
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise last_error


def choose_output_stem(buyer, amount, original_img_name, invoice_number):
    if buyer and amount:
        return f"{sanitize_filename(buyer)}-{amount}"
    if buyer:
        return f"{sanitize_filename(buyer)}-{invoice_number}"
    if amount:
        return f"{original_img_name}-{amount}"
    return f"{original_img_name}-{invoice_number}"


def build_final_filename(temp_path, original_img_name, invoice_number):
    buyer, amount, _extracted_invoice_number = extract_invoice_metadata(temp_path)
    return f"{choose_output_stem(buyer, amount, original_img_name, invoice_number)}.pdf"


def extract_invoice_number_from_pdf(pdf_path):
    try:
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for page in pdf.pages[:2]:
                try:
                    text += "\n" + (page.extract_text() or "")
                except Exception:
                    continue
        if not text:
            return None

        patterns = [
            r"(?:发票号码|票据号码|Invoice\s*No\.?)[:：]?\s*([0-9]{8,20})",
            r"号码[:：]?\s*([0-9]{8,20})",
        ]
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.IGNORECASE)
            if match:
                return match.group(1).strip()
    except Exception as exc:
        print(f"解析 PDF 发票号码失败: {exc}")

    return None


def build_unique_output_path(output_dir, desired_name):
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    stem, ext = os.path.splitext(desired_name)
    candidate = os.path.join(output_dir, desired_name)
    if not os.path.exists(candidate):
        return candidate

    timestamp = int(time.time())
    counter = 1
    while True:
        suffix = f"_{timestamp}" if counter == 1 else f"_{timestamp}_{counter}"
        candidate = os.path.join(output_dir, f"{stem}{suffix}{ext}")
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def copy_pdf_with_generated_name(source_pdf_path, output_dir, original_name_hint=None):
    if not os.path.isfile(source_pdf_path):
        raise FileNotFoundError(source_pdf_path)

    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    source_pdf_path = os.path.abspath(source_pdf_path)
    name_hint = sanitize_filename(original_name_hint or os.path.splitext(os.path.basename(source_pdf_path))[0]) or "invoice"
    buyer, amount, invoice_number = extract_invoice_metadata(source_pdf_path)
    invoice_number = invoice_number or name_hint
    final_name = f"{choose_output_stem(buyer, amount, name_hint, invoice_number)}.pdf"
    final_path = build_unique_output_path(output_dir, final_name)
    shutil.copy2(source_pdf_path, final_path)
    return {
        "status": "success",
        "source_path": source_pdf_path,
        "file_name": os.path.basename(final_path),
        "file_path": final_path,
        "invoice_number": invoice_number,
    }


def download_and_rename_to_output(url, original_img_name, output_dir):
    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)

    temp_path = None
    try:
        temp_path, cs_value, invoice_number = download_invoice_pdf(url)
        final_name = build_final_filename(temp_path, original_img_name, invoice_number)
        final_path = build_unique_output_path(output_dir, final_name)
        os.replace(temp_path, final_path)
        return {
            "status": "success",
            "cs": cs_value,
            "invoice_number": invoice_number,
            "file_name": os.path.basename(final_path),
            "file_path": final_path,
        }
    except Exception as exc:
        if temp_path and os.path.exists(temp_path):
            os.remove(temp_path)
        return {"status": "failed", "error": str(exc)}


def load_processed_index():
    if not os.path.exists(INDEX_FILE):
        return {}

    try:
        with open(INDEX_FILE, "r", encoding="utf-8") as file_obj:
            data = json.load(file_obj)
        if isinstance(data, dict):
            return data
    except Exception as exc:
        print(f"读取已处理索引失败，将重新创建: {exc}")

    return {}


def save_processed_index(index_data):
    with open(INDEX_FILE, "w", encoding="utf-8") as file_obj:
        json.dump(index_data, file_obj, ensure_ascii=False, indent=2, sort_keys=True)


def write_failed_log(failures):
    if not failures:
        if os.path.exists(FAILED_LOG_FILE):
            os.remove(FAILED_LOG_FILE)
        return

    with open(FAILED_LOG_FILE, "w", encoding="utf-8") as file_obj:
        for item in failures:
            file_obj.write(f"{item['file']}\t{item['url']}\t{item['error']}\n")


def decode_qr_image(image_path):
    with Image.open(image_path) as image:
        results = decode(image)
    if not results:
        return None
    return results[0].data.decode("utf-8")


def download_and_rename(url, original_img_name):
    result = download_and_rename_to_output(url, original_img_name, OUTPUT_DIR)
    if result["status"] == "success":
        return {
            "status": "success",
            "cs": result["cs"],
            "invoice_number": result["invoice_number"],
            "file_name": result["file_name"],
        }
    return result


def file_sha256(file_path):
    digest = hashlib.sha256()
    with open(file_path, "rb") as file_obj:
        for chunk in iter(lambda: file_obj.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def list_pdf_files(output_dir):
    if not os.path.isdir(output_dir):
        return []

    file_names = []
    for name in os.listdir(output_dir):
        path = os.path.join(output_dir, name)
        if os.path.isfile(path) and name.lower().endswith(".pdf"):
            file_names.append(name)
    return sorted(file_names)


def choose_duplicate_keeper(file_names, preferred_name=None):
    if preferred_name and preferred_name in file_names:
        return preferred_name

    plain_names = [name for name in file_names if not TIMESTAMPED_PDF_RE.match(name)]
    candidates = plain_names or list(file_names)
    return min(candidates, key=lambda name: (len(name), name))


def find_duplicate_pdfs(output_dir, processed_index):
    groups_by_hash = {}
    preferred_names = {item.get("file_name") for item in processed_index.values() if item.get("file_name")}

    for name in list_pdf_files(output_dir):
        file_path = os.path.join(output_dir, name)
        file_hash = file_sha256(file_path)
        groups_by_hash.setdefault(file_hash, []).append(name)

    duplicates = []
    for file_hash, names in sorted(groups_by_hash.items()):
        if len(names) < 2:
            continue

        preferred_name = next((name for name in names if name in preferred_names), None)
        keep_name = choose_duplicate_keeper(names, preferred_name=preferred_name)
        remove_names = sorted(name for name in names if name != keep_name)
        duplicates.append(
            {
                "hash": file_hash,
                "keep": keep_name,
                "remove": remove_names,
            }
        )

    return duplicates


def cleanup_duplicate_pdfs(output_dir, dry_run=False):
    processed_index = load_processed_index()
    duplicates = find_duplicate_pdfs(output_dir, processed_index)
    removed_files = []

    if not duplicates:
        print("未发现重复 PDF。")
        return {"groups": 0, "removed": 0, "removed_files": removed_files}

    print(f"发现 {len(duplicates)} 组重复 PDF。")
    for group in duplicates:
        print(f"保留: {group['keep']}")
        for name in group["remove"]:
            print(f"{'预览删除' if dry_run else '删除'}: {name}")
            if not dry_run:
                os.remove(os.path.join(output_dir, name))
            removed_files.append(name)

    if dry_run:
        print(f"预览完成，共 {len(removed_files)} 个文件可清理。")
    else:
        print(f"清理完成，共删除 {len(removed_files)} 个重复文件。")

    return {
        "groups": len(duplicates),
        "removed": len(removed_files),
        "removed_files": removed_files,
    }


def clear_directory_contents(target_dir):
    target_dir = os.path.abspath(target_dir)
    if not os.path.exists(target_dir):
        print(f"目录不存在，无需清空: {target_dir}")
        return {"removed_files": 0, "removed_dirs": 0}
    if not os.path.isdir(target_dir):
        raise ValueError(f"目标路径不是目录: {target_dir}")

    removed_files = 0
    removed_dirs = 0

    for current_root, dir_names, file_names in os.walk(target_dir, topdown=False):
        for file_name in file_names:
            file_path = os.path.join(current_root, file_name)
            os.remove(file_path)
            removed_files += 1
            print(f"删除文件: {file_path}")

        for dir_name in dir_names:
            dir_path = os.path.join(current_root, dir_name)
            os.rmdir(dir_path)
            removed_dirs += 1
            print(f"删除文件夹: {dir_path}")

    print(f"清空完成: 删除 {removed_files} 个文件，删除 {removed_dirs} 个子文件夹。")
    return {"removed_files": removed_files, "removed_dirs": removed_dirs}


def print_summary(stats, failures):
    print("")
    print("处理完成。")
    print(f"图片总数: {stats['total_images']}")
    print(f"识别成功: {stats['decoded']}")
    print(f"下载成功: {stats['success']}")
    print(f"已跳过: {stats['skipped']}")
    print(f"失败数量: {stats['failed']}")

    if failures:
        print(f"失败清单已写入: {FAILED_LOG_FILE}")
        for item in failures:
            print(f"- {item['file']}: {item['error']}")
    else:
        print("无失败项。")


def run_download_flow():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if not os.path.isdir(INPUT_DIR):
        print(f"输入目录不存在: {INPUT_DIR}")
        return

    processed_index = load_processed_index()
    files = sorted(
        [name for name in os.listdir(INPUT_DIR) if name.lower().endswith((".png", ".jpg", ".jpeg"))]
    )
    if not files:
        print("未在 qrcode_images 目录找到图片，程序结束。")
        return

    tasks = []
    stats = {
        "total_images": len(files),
        "decoded": 0,
        "skipped": 0,
        "success": 0,
        "failed": 0,
    }
    failures = []

    for filename in files:
        try:
            image_path = os.path.join(INPUT_DIR, filename)
            url = decode_qr_image(image_path)
            if not url:
                stats["failed"] += 1
                failures.append({"file": filename, "url": "", "error": "未识别到二维码"})
                print(f"未能从 {filename} 识别到二维码。")
                continue

            stats["decoded"] += 1
            cs_value = extract_cs_from_url(url)
            if cs_value and cs_value in processed_index:
                stats["skipped"] += 1
                print(f"跳过已处理发票: {filename} -> {processed_index[cs_value]['file_name']}")
                continue

            tasks.append((filename, url))
        except Exception as exc:
            stats["failed"] += 1
            failures.append({"file": filename, "url": "", "error": str(exc)})
            print(f"处理文件 {filename} 时出错: {exc}")

    if not tasks:
        write_failed_log(failures)
        print("没有新的二维码需要下载。")
        print_summary(stats, failures)
        return

    print(f"共识别到 {stats['decoded']} 张二维码，其中 {len(tasks)} 张待下载。")
    for index, (filename, url) in enumerate(tasks, start=1):
        print(f"[{index}/{len(tasks)}] 处理 {filename} -> {url}")
        result = download_and_rename(url, os.path.splitext(filename)[0])
        if result["status"] == "success":
            stats["success"] += 1
            processed_index[result["cs"]] = {
                "file_name": result["file_name"],
                "invoice_number": result["invoice_number"],
                "source_image": filename,
                "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            save_processed_index(processed_index)
            print(f"【成功】发票已保存为: {result['file_name']}")
        else:
            stats["failed"] += 1
            failures.append({"file": filename, "url": url, "error": result["error"]})
            print(f"处理失败: {result['error']}")

    write_failed_log(failures)
    print_summary(stats, failures)


def parse_args():
    parser = argparse.ArgumentParser(description="批量下载发票并管理重复 PDF。")
    parser.add_argument(
        "--cleanup-duplicates",
        action="store_true",
        help="按文件内容清理 invoice_pdfs 里的重复 PDF。",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="配合 --cleanup-duplicates 使用，只预览将删除的文件。",
    )
    return parser.parse_args()


def cli_main():
    enable_utf8_console()
    args = parse_args()
    if args.cleanup_duplicates:
        cleanup_duplicate_pdfs(OUTPUT_DIR, dry_run=args.dry_run)
        return
    run_download_flow()


if __name__ == "__main__":
    try:
        cli_main()
    except Exception:
        import traceback

        traceback.print_exc()
