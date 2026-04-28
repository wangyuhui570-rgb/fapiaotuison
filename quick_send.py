import mimetypes
import os
import shutil
import tempfile
import time
import zipfile
from urllib.parse import parse_qs, urlparse, urlunparse

import requests

import main

IMAGE_EXTENSIONS = (".png", ".jpg", ".jpeg", ".bmp", ".webp")
PDF_EXTENSIONS = (".pdf",)
ZIP_EXTENSIONS = (".zip",)
PUSH_FAILED_LOG_NAME = "push_failed.txt"
DEFAULT_WECOM_SEND_INTERVAL_SECONDS = 2.0
WECOM_RATE_LIMIT_BACKOFF_SECONDS = 2.0
WECOM_RATE_LIMIT_MAX_RETRIES = 3
WECOM_RATE_LIMIT_ERRCODE = 45009
BATCH_ZIP_DATE_FORMAT = "%y%m%d"
BATCH_ZIP_DATETIME_FORMAT = "%y%m%d%H%M"
DEFAULT_BATCH_SUMMARY_TEMPLATE = (
    "本轮发票已推送完成，共{count}张，截止到{cutoff_date}以及之前的发票。"
    "可以通过搜索发票抬头或者发票金额查询相应发票，"
    "店长上传发票可以下载压缩包到本地搜索相应发票上传。"
)


class WeComRateLimitError(Exception):
    pass


def is_supported_image_file(file_path):
    return os.path.isfile(file_path) and file_path.lower().endswith(IMAGE_EXTENSIONS)


def is_supported_pdf_file(file_path):
    return os.path.isfile(file_path) and file_path.lower().endswith(PDF_EXTENSIONS)


def is_supported_input_file(file_path):
    return is_supported_pdf_file(file_path) or is_supported_image_file(file_path)


def is_supported_zip_file(file_path):
    return os.path.isfile(file_path) and file_path.lower().endswith(ZIP_EXTENSIONS)


def collect_supported_input_files(paths):
    collected = []
    seen = set()

    def add_file(file_path):
        absolute = os.path.abspath(file_path)
        if absolute in seen or not is_supported_input_file(absolute):
            return
        seen.add(absolute)
        collected.append(absolute)

    for path in paths:
        if not path:
            continue
        absolute = os.path.abspath(path)
        if os.path.isfile(absolute):
            add_file(absolute)
            continue
        if not os.path.isdir(absolute):
            continue

        for current_root, dir_names, file_names in os.walk(absolute):
            dir_names.sort()
            for file_name in sorted(file_names):
                add_file(os.path.join(current_root, file_name))

    return collected


def collect_supported_push_input_files(paths):
    collected = []
    seen = set()

    def add_file(file_path):
        absolute = os.path.abspath(file_path)
        if absolute in seen:
            return
        if not (is_supported_input_file(absolute) or is_supported_zip_file(absolute)):
            return
        seen.add(absolute)
        collected.append(absolute)

    for path in paths:
        if not path:
            continue
        absolute = os.path.abspath(path)
        if os.path.isfile(absolute):
            add_file(absolute)
            continue
        if not os.path.isdir(absolute):
            continue

        for current_root, dir_names, file_names in os.walk(absolute):
            dir_names.sort()
            for file_name in sorted(file_names):
                add_file(os.path.join(current_root, file_name))

    return collected


def push_failed_log_path(base_dir):
    target_dir = os.path.abspath(base_dir or os.getcwd())
    os.makedirs(target_dir, exist_ok=True)
    return os.path.join(target_dir, PUSH_FAILED_LOG_NAME)


def write_push_failed_log(failures, base_dir):
    log_path = push_failed_log_path(base_dir)
    if not failures:
        if os.path.exists(log_path):
            os.remove(log_path)
        return log_path

    with open(log_path, "w", encoding="utf-8") as file_obj:
        for item in failures:
            file_obj.write(
                "\t".join(
                    [
                        str(item.get("stage", "")),
                        str(item.get("source_path", "")),
                        str(item.get("error", "")),
                    ]
                )
            )
            file_obj.write("\n")
    return log_path


def ensure_failed_push_dir(base_dir):
    root_dir = os.path.abspath(base_dir or os.getcwd())
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    target_dir = os.path.join(root_dir, f"push_retry_failed_{timestamp}")
    os.makedirs(target_dir, exist_ok=True)
    return target_dir


def persist_failed_pdf(source_file_path, target_dir):
    os.makedirs(target_dir, exist_ok=True)
    file_name = os.path.basename(source_file_path)
    stem, ext = os.path.splitext(file_name)
    destination_path = os.path.join(target_dir, file_name)
    counter = 1
    while os.path.exists(destination_path):
        destination_path = os.path.join(target_dir, f"{stem}_{counter}{ext}")
        counter += 1
    shutil.copy2(source_file_path, destination_path)
    return destination_path


def _safe_extract_zip(zip_path, target_dir):
    target_root = os.path.abspath(target_dir)
    with zipfile.ZipFile(zip_path) as zip_file:
        for member in zip_file.infolist():
            member_path = os.path.abspath(os.path.join(target_root, member.filename))
            if member_path != target_root and not member_path.startswith(target_root + os.sep):
                raise ValueError(f"ZIP 包含非法路径: {member.filename}")
        zip_file.extractall(target_root)


def expand_push_inputs(paths, extraction_root, logger=print, failures=None):
    expanded_paths = []
    for source_path in collect_supported_push_input_files(paths):
        if not is_supported_zip_file(source_path):
            expanded_paths.append(source_path)
            continue

        extract_dir = os.path.join(
            extraction_root,
            f"zip_{len(expanded_paths)}_{os.path.splitext(os.path.basename(source_path))[0]}",
        )
        try:
            os.makedirs(extract_dir, exist_ok=True)
            logger(f"解压 ZIP: {source_path}")
            _safe_extract_zip(source_path, extract_dir)
            extracted_files = collect_supported_input_files([extract_dir])
            if not extracted_files:
                raise ValueError("ZIP 包内未找到可推送的 PDF 或二维码图片。")
            expanded_paths.extend(extracted_files)
            logger(f"ZIP 内识别到 {len(extracted_files)} 个可处理文件。")
        except Exception as exc:
            if failures is not None:
                failures.append(
                    {
                        "stage": "extract_zip",
                        "source_path": source_path,
                        "error": str(exc),
                    }
                )
            logger(f"处理失败: {source_path} -> {exc}")

    return expanded_paths


def extract_webhook_key(webhook_url):
    parsed = urlparse((webhook_url or "").strip())
    return (parse_qs(parsed.query).get("key") or [""])[0].strip()


def build_webhook_upload_url(webhook_url):
    parsed = urlparse((webhook_url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("Webhook URL 无效。")

    key = extract_webhook_key(webhook_url)
    if not key:
        raise ValueError("Webhook URL 缺少 key 参数。")

    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            "/cgi-bin/webhook/upload_media",
            "",
            f"key={key}&type=file",
            "",
        )
    )


def build_file_message_payload(media_id):
    return {
        "msgtype": "file",
        "file": {"media_id": media_id},
    }


def build_text_message_payload(content):
    return {
        "msgtype": "text",
        "text": {"content": content},
    }


def default_batch_cutoff_date():
    return time.strftime(BATCH_ZIP_DATE_FORMAT)


def normalize_batch_cutoff_date(value):
    raw_value = str(value or "").strip()
    if not raw_value:
        return default_batch_cutoff_date()

    digits_only = "".join(ch for ch in raw_value if ch.isdigit())
    if len(digits_only) == 6:
        return digits_only
    if len(digits_only) == 8:
        return digits_only[2:]
    if len(digits_only) == 10:
        return digits_only
    if len(digits_only) == 12:
        return digits_only[2:]
    raise ValueError("发票截止时间格式无效，请填写 YYMMDD、YYYYMMDD、YYMMDDHHMM 或 YYYYMMDDHHMM。")


def format_batch_cutoff_date(batch_cutoff_date=None, for_filename=False):
    normalized = normalize_batch_cutoff_date(batch_cutoff_date)
    if len(normalized) == 10:
        text = (
            f"20{normalized[0:2]}-{normalized[2:4]}-{normalized[4:6]} "
            f"{normalized[6:8]}:{normalized[8:10]}"
        )
    else:
        text = f"20{normalized[0:2]}-{normalized[2:4]}-{normalized[4:6]}"
    if for_filename:
        return text.replace(":", "-").replace(" ", "_")
    return text


def _normalize_send_interval(send_interval_seconds):
    try:
        value = float(send_interval_seconds)
    except (TypeError, ValueError):
        value = DEFAULT_WECOM_SEND_INTERVAL_SECONDS
    return max(0.0, value)


def _is_wecom_rate_limit(result):
    errcode = result.get("errcode")
    errmsg = str(result.get("errmsg") or "")
    return str(errcode) == str(WECOM_RATE_LIMIT_ERRCODE) or "api freq out of limit" in errmsg.lower()


def _raise_wecom_result_error(action_name, result):
    errmsg = str(result.get("errmsg") or result)
    if _is_wecom_rate_limit(result):
        raise WeComRateLimitError(f"{action_name}: {errmsg}")
    raise ValueError(f"{action_name}: {errmsg}")


def send_file_via_wecom_webhook(webhook_url, file_path, timeout=20, session=None):
    if not os.path.isfile(file_path):
        raise FileNotFoundError(file_path)

    webhook_url = (webhook_url or "").strip()
    upload_url = build_webhook_upload_url(webhook_url)
    own_session = session is None
    session = session or requests.Session()

    try:
        content_type = mimetypes.guess_type(file_path)[0] or "application/octet-stream"
        with open(file_path, "rb") as file_obj:
            upload_response = session.post(
                upload_url,
                files={"media": (os.path.basename(file_path), file_obj, content_type)},
                timeout=timeout,
            )
        upload_response.raise_for_status()
        upload_result = upload_response.json()
        if upload_result.get("errcode") not in (0, "0", None):
            _raise_wecom_result_error("上传文件失败", upload_result)

        media_id = (upload_result.get("media_id") or "").strip()
        if not media_id:
            raise ValueError("上传文件成功但未返回 media_id。")

        send_response = session.post(
            webhook_url,
            json=build_file_message_payload(media_id),
            timeout=timeout,
        )
        send_response.raise_for_status()
        send_result = send_response.json()
        if send_result.get("errcode") not in (0, "0", None):
            _raise_wecom_result_error("发送文件消息失败", send_result)
        return send_result
    finally:
        if own_session:
            session.close()


def send_text_via_wecom_webhook(webhook_url, content, timeout=20, session=None):
    webhook_url = (webhook_url or "").strip()
    parsed = urlparse(webhook_url)
    if not parsed.scheme or not parsed.netloc:
        raise ValueError("Webhook URL 无效。")

    own_session = session is None
    session = session or requests.Session()

    try:
        send_response = session.post(
            webhook_url,
            json=build_text_message_payload(content),
            timeout=timeout,
        )
        send_response.raise_for_status()
        send_result = send_response.json()
        if send_result.get("errcode") not in (0, "0", None):
            _raise_wecom_result_error("发送文本消息失败", send_result)
        return send_result
    finally:
        if own_session:
            session.close()


def send_file_via_wecom_webhook_with_retry(
    webhook_url,
    file_path,
    timeout=20,
    session=None,
    logger=print,
    backoff_seconds=WECOM_RATE_LIMIT_BACKOFF_SECONDS,
    max_rate_limit_retries=WECOM_RATE_LIMIT_MAX_RETRIES,
):
    retries_used = 0
    for attempt in range(max_rate_limit_retries + 1):
        try:
            send_file_via_wecom_webhook(
                webhook_url,
                file_path,
                timeout=timeout,
                session=session,
            )
            return retries_used
        except WeComRateLimitError as exc:
            if attempt >= max_rate_limit_retries:
                raise
            retries_used += 1
            logger(
                f"触发企业微信限流，{backoff_seconds:.0f} 秒后重试 "
                f"({retries_used}/{max_rate_limit_retries}): {os.path.basename(file_path)}"
            )
            time.sleep(backoff_seconds)


def send_text_via_wecom_webhook_with_retry(
    webhook_url,
    content,
    timeout=20,
    session=None,
    logger=print,
    backoff_seconds=WECOM_RATE_LIMIT_BACKOFF_SECONDS,
    max_rate_limit_retries=WECOM_RATE_LIMIT_MAX_RETRIES,
):
    retries_used = 0
    for attempt in range(max_rate_limit_retries + 1):
        try:
            send_text_via_wecom_webhook(
                webhook_url,
                content,
                timeout=timeout,
                session=session,
            )
            return retries_used
        except WeComRateLimitError:
            if attempt >= max_rate_limit_retries:
                raise
            retries_used += 1
            logger(
                f"触发企业微信限流，{backoff_seconds:.0f} 秒后重试文本消息 "
                f"({retries_used}/{max_rate_limit_retries})"
            )
            time.sleep(backoff_seconds)


def build_batch_zip_name(batch_cutoff_date=None):
    return f"发票截止{format_batch_cutoff_date(batch_cutoff_date, for_filename=True)}.zip"


def normalize_batch_summary_template(template):
    text = str(template or "").strip()
    if not text:
        return DEFAULT_BATCH_SUMMARY_TEMPLATE
    return text


def build_batch_summary_text(invoice_count, batch_cutoff_date=None, template=None):
    cutoff_date = format_batch_cutoff_date(batch_cutoff_date)
    return (
        normalize_batch_summary_template(template)
        .replace("{count}", str(invoice_count))
        .replace("{cutoff_date}", cutoff_date)
    )


def create_batch_pdf_zip(pdf_paths, target_dir, batch_cutoff_date=None):
    os.makedirs(target_dir, exist_ok=True)
    zip_name = build_batch_zip_name(batch_cutoff_date)
    zip_path = os.path.join(target_dir, zip_name)
    stem, ext = os.path.splitext(zip_name)
    counter = 1
    while os.path.exists(zip_path):
        zip_path = os.path.join(target_dir, f"{stem}_{counter}{ext}")
        counter += 1

    used_names = set()
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_file:
        for pdf_path in pdf_paths:
            archive_name = os.path.basename(pdf_path)
            name_stem, name_ext = os.path.splitext(archive_name)
            unique_name = archive_name
            index = 1
            while unique_name in used_names:
                unique_name = f"{name_stem}_{index}{name_ext}"
                index += 1
            used_names.add(unique_name)
            zip_file.write(pdf_path, arcname=unique_name)
    return zip_path


def process_inputs_locally(paths, output_dir, logger=print):
    file_paths = collect_supported_input_files(paths)
    if not file_paths:
        raise ValueError("未找到可处理的 PDF 或二维码图片。")

    output_dir = os.path.abspath(output_dir)
    os.makedirs(output_dir, exist_ok=True)
    main.configure_paths(output_dir=output_dir)
    processed_index = main.load_processed_index()

    summary = {
        "mode": "local",
        "total": len(file_paths),
        "pdf_inputs": 0,
        "image_inputs": 0,
        "success": 0,
        "skipped": 0,
        "failed": 0,
        "generated_files": [],
        "errors": [],
    }
    failures = []

    for index, source_path in enumerate(file_paths, start=1):
        source_url = ""
        logger(f"[{index}/{len(file_paths)}] 处理 {source_path}")
        try:
            if is_supported_pdf_file(source_path):
                summary["pdf_inputs"] += 1
                result = main.copy_pdf_with_generated_name(source_path, output_dir)
            else:
                summary["image_inputs"] += 1
                source_url = main.decode_qr_image(source_path)
                if not source_url:
                    raise ValueError("未识别到二维码。")

                cs_value = main.extract_cs_from_url(source_url)
                if cs_value and cs_value in processed_index:
                    summary["skipped"] += 1
                    existing_name = processed_index[cs_value].get("file_name") or cs_value
                    logger(f"跳过已处理二维码: {os.path.basename(source_path)} -> {existing_name}")
                    continue

                result = main.download_and_rename_to_output(
                    source_url,
                    os.path.splitext(os.path.basename(source_path))[0],
                    output_dir,
                )
                if result["status"] != "success":
                    raise ValueError(result["error"])

                if result.get("cs"):
                    processed_index[result["cs"]] = {
                        "file_name": result["file_name"],
                        "invoice_number": result["invoice_number"],
                        "source_image": os.path.basename(source_path),
                        "processed_at": main.time.strftime("%Y-%m-%d %H:%M:%S"),
                    }
                    main.save_processed_index(processed_index)

            generated_path = result["file_path"]
            summary["success"] += 1
            summary["generated_files"].append(generated_path)
            logger(f"已保存 {os.path.basename(generated_path)}")
        except Exception as exc:
            summary["failed"] += 1
            summary["errors"].append({"source_path": source_path, "error": str(exc)})
            failures.append(
                {
                    "file": os.path.basename(source_path),
                    "url": source_url,
                    "error": str(exc),
                }
            )
            logger(f"处理失败: {source_path} -> {exc}")

    main.write_failed_log(failures)
    logger(
        "本地处理完成："
        f"共 {summary['total']} 个输入，"
        f"PDF {summary['pdf_inputs']} 个，"
        f"二维码 {summary['image_inputs']} 个，"
        f"成功 {summary['success']} 个，"
        f"跳过 {summary['skipped']} 个，"
        f"失败 {summary['failed']} 个。"
    )
    return summary


def process_inputs_and_send(
    paths,
    output_dir,
    webhook_url,
    logger=print,
    send_interval_seconds=DEFAULT_WECOM_SEND_INTERVAL_SECONDS,
    batch_cutoff_date=None,
    batch_summary_template=None,
):
    log_base_dir = output_dir or os.getcwd()
    send_interval_seconds = _normalize_send_interval(send_interval_seconds)
    batch_cutoff_date = normalize_batch_cutoff_date(batch_cutoff_date)
    batch_summary_template = normalize_batch_summary_template(batch_summary_template)

    failures = []
    resend_queue = []
    successful_pdf_paths = []
    sent_invoice_numbers = set()
    sent_qr_cs_values = set()

    summary = {
        "mode": "push",
        "total": 0,
        "pdf_inputs": 0,
        "image_inputs": 0,
        "sent": 0,
        "failed": 0,
        "generated_files": [],
        "errors": [],
        "send_interval_seconds": send_interval_seconds,
        "rate_limit_retries": 0,
        "resend_queued": 0,
        "resend_sent": 0,
        "resend_failed": 0,
        "manual_retry_dir": "",
        "manual_retry_files": [],
        "batch_zip_name": "",
        "batch_zip_sent": False,
        "summary_text_sent": False,
        "post_push_failed": 0,
        "skipped_duplicates": 0,
        "batch_cutoff_date": batch_cutoff_date,
        "batch_summary_template": batch_summary_template,
    }

    with tempfile.TemporaryDirectory(prefix="invoice_push_") as temp_output_dir:
        file_paths = expand_push_inputs(
            paths,
            os.path.join(temp_output_dir, "_unzipped"),
            logger=logger,
            failures=failures,
        )
        if not file_paths:
            if failures:
                summary["failed"] = len(failures)
                summary["errors"].extend(failures)
                write_push_failed_log(failures, log_base_dir)
                logger(f"推送失败明细已写入 {push_failed_log_path(log_base_dir)}")
            raise ValueError("未找到可处理的 PDF、二维码图片或 ZIP 压缩包内容。")

        summary["total"] = len(file_paths) + len(failures)
        summary["failed"] = len(failures)
        summary["errors"].extend(failures)

        for index, source_path in enumerate(file_paths, start=1):
            logger(f"[{index}/{len(file_paths)}] 处理 {source_path}")
            stage = "prepare"
            generated_path = ""
            current_cs_value = ""
            invoice_number = ""
            attempted_send = False
            try:
                if is_supported_pdf_file(source_path):
                    stage = "rename_pdf"
                    summary["pdf_inputs"] += 1
                    result = main.copy_pdf_with_generated_name(source_path, temp_output_dir)
                else:
                    stage = "decode_qr"
                    summary["image_inputs"] += 1
                    url = main.decode_qr_image(source_path)
                    if not url:
                        raise ValueError("未识别到二维码。")

                    current_cs_value = main.extract_cs_from_url(url)
                    if current_cs_value and current_cs_value in sent_qr_cs_values:
                        summary["skipped_duplicates"] += 1
                        logger(f"跳过重复二维码: {os.path.basename(source_path)}")
                        continue

                    stage = "download_pdf"
                    result = main.download_and_rename_to_output(
                        url,
                        os.path.splitext(os.path.basename(source_path))[0],
                        temp_output_dir,
                    )
                    if result["status"] != "success":
                        raise ValueError(result["error"])

                generated_path = result["file_path"]
                invoice_number = str(result.get("invoice_number") or "").strip()
                if invoice_number and invoice_number in sent_invoice_numbers:
                    summary["skipped_duplicates"] += 1
                    logger(f"跳过重复发票: {os.path.basename(generated_path)}")
                    continue

                attempted_send = True
                stage = "send_wecom"
                retries_used = send_file_via_wecom_webhook_with_retry(
                    webhook_url,
                    generated_path,
                    logger=logger,
                )
                summary["rate_limit_retries"] += retries_used
                summary["sent"] += 1
                summary["generated_files"].append(os.path.basename(generated_path))
                if invoice_number:
                    sent_invoice_numbers.add(invoice_number)
                if current_cs_value:
                    sent_qr_cs_values.add(current_cs_value)
                if generated_path not in successful_pdf_paths:
                    successful_pdf_paths.append(generated_path)
                logger(f"已发送 {os.path.basename(generated_path)}")
            except Exception as exc:
                if stage == "send_wecom" and generated_path:
                    resend_queue.append(
                        {
                            "stage": stage,
                            "source_path": source_path,
                            "generated_path": generated_path,
                            "invoice_number": invoice_number,
                            "cs": current_cs_value,
                            "error": str(exc),
                        }
                    )
                    summary["resend_queued"] += 1
                    logger(f"首次发送失败，已加入失败 PDF 补发队列: {generated_path} -> {exc}")
                else:
                    failure_item = {
                        "stage": stage,
                        "source_path": source_path,
                        "error": str(exc),
                    }
                    failures.append(failure_item)
                    summary["failed"] += 1
                    summary["errors"].append(failure_item)
                    logger(f"处理失败: {source_path} -> {exc}")
            finally:
                if attempted_send and index < len(file_paths):
                    logger(f"推送间隔 {send_interval_seconds:.1f} 秒")
                    time.sleep(send_interval_seconds)

        if resend_queue:
            logger(f"开始补发失败 PDF，共 {len(resend_queue)} 个。")

        for index, item in enumerate(resend_queue, start=1):
            generated_path = item["generated_path"]
            invoice_number = str(item.get("invoice_number") or "").strip()
            current_cs_value = str(item.get("cs") or "").strip()
            logger(f"[补发 {index}/{len(resend_queue)}] {generated_path}")
            attempted_send = False
            try:
                if invoice_number and invoice_number in sent_invoice_numbers:
                    summary["skipped_duplicates"] += 1
                    logger(f"跳过重复补发发票: {os.path.basename(generated_path)}")
                    continue
                if current_cs_value and current_cs_value in sent_qr_cs_values:
                    summary["skipped_duplicates"] += 1
                    logger(f"跳过重复补发二维码: {os.path.basename(generated_path)}")
                    continue

                attempted_send = True
                retries_used = send_file_via_wecom_webhook_with_retry(
                    webhook_url,
                    generated_path,
                    logger=logger,
                )
                summary["rate_limit_retries"] += retries_used
                summary["sent"] += 1
                summary["resend_sent"] += 1
                summary["generated_files"].append(os.path.basename(generated_path))
                if invoice_number:
                    sent_invoice_numbers.add(invoice_number)
                if current_cs_value:
                    sent_qr_cs_values.add(current_cs_value)
                if generated_path not in successful_pdf_paths:
                    successful_pdf_paths.append(generated_path)
                logger(f"补发成功 {os.path.basename(generated_path)}")
            except Exception as exc:
                manual_retry_path = ""
                if generated_path:
                    if not summary["manual_retry_dir"]:
                        summary["manual_retry_dir"] = ensure_failed_push_dir(log_base_dir)
                    manual_retry_path = persist_failed_pdf(generated_path, summary["manual_retry_dir"])
                    summary["manual_retry_files"].append(manual_retry_path)
                failure_item = {
                    "stage": item["stage"],
                    "source_path": item["source_path"],
                    "error": str(exc),
                }
                if manual_retry_path:
                    failure_item["saved_pdf_path"] = manual_retry_path
                failures.append(failure_item)
                summary["failed"] += 1
                summary["resend_failed"] += 1
                summary["errors"].append(failure_item)
                logger(f"补发失败: {generated_path} -> {exc}")
            finally:
                if attempted_send and index < len(resend_queue):
                    logger(f"推送间隔 {send_interval_seconds:.1f} 秒")
                    time.sleep(send_interval_seconds)
        if successful_pdf_paths:
            batch_zip_path = create_batch_pdf_zip(
                successful_pdf_paths,
                temp_output_dir,
                batch_cutoff_date=batch_cutoff_date,
            )
            summary["batch_zip_name"] = os.path.basename(batch_zip_path)
            try:
                retries_used = send_file_via_wecom_webhook_with_retry(
                    webhook_url,
                    batch_zip_path,
                    logger=logger,
                )
                summary["rate_limit_retries"] += retries_used
                summary["batch_zip_sent"] = True
                logger(f"已发送批量 ZIP: {summary['batch_zip_name']}")
            except Exception as exc:
                summary["post_push_failed"] += 1
                failure_item = {
                    "stage": "send_batch_zip",
                    "source_path": batch_zip_path,
                    "error": str(exc),
                }
                failures.append(failure_item)
                summary["errors"].append(failure_item)
                logger(f"发送批量 ZIP 失败: {batch_zip_path} -> {exc}")

            if summary["batch_zip_sent"]:
                summary_text = build_batch_summary_text(
                    len(successful_pdf_paths),
                    batch_cutoff_date=batch_cutoff_date,
                    template=batch_summary_template,
                )
                try:
                    retries_used = send_text_via_wecom_webhook_with_retry(
                        webhook_url,
                        summary_text,
                        logger=logger,
                    )
                    summary["rate_limit_retries"] += retries_used
                    summary["summary_text_sent"] = True
                    logger("已发送批次完成提醒。")
                except Exception as exc:
                    summary["post_push_failed"] += 1
                    failure_item = {
                        "stage": "send_summary_text",
                        "source_path": summary["batch_zip_name"],
                        "error": str(exc),
                    }
                    failures.append(failure_item)
                    summary["errors"].append(failure_item)
                    logger(f"发送批次完成提醒失败: {exc}")

    write_push_failed_log(failures, log_base_dir)
    if failures:
        logger(f"推送失败明细已写入 {push_failed_log_path(log_base_dir)}")
    logger(
        "推送完成："
        f"共 {summary['total']} 个输入，"
        f"PDF {summary['pdf_inputs']} 个，"
        f"二维码 {summary['image_inputs']} 个，"
        f"成功发送 {summary['sent']} 个，"
        f"失败 {summary['failed']} 个，"
        f"进入补发队列 {summary['resend_queued']} 个，"
        f"补发成功 {summary['resend_sent']} 个，"
        f"补发失败 {summary['resend_failed']} 个，"
        f"限流重试 {summary['rate_limit_retries']} 次。"
    )
    if summary["manual_retry_dir"]:
        logger(
            f"补发仍失败的 PDF 已保存到 {summary['manual_retry_dir']}，"
            f"共 {len(summary['manual_retry_files'])} 个。"
        )
    return summary
