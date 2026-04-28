import os
import tempfile
import unittest
import zipfile
from unittest import mock

import quick_send


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self):
        self.calls = []
        self.closed = False

    def post(self, url, files=None, json=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "files": files,
                "json": json,
                "timeout": timeout,
            }
        )
        if files is not None:
            return FakeResponse({"errcode": 0, "media_id": "media-123"})
        return FakeResponse({"errcode": 0, "errmsg": "ok"})

    def close(self):
        self.closed = True


class QuickSendTests(unittest.TestCase):
    def test_collect_supported_input_files_includes_files_from_dirs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = os.path.join(temp_dir, "a.pdf")
            image_path = os.path.join(temp_dir, "b.png")
            ignored_path = os.path.join(temp_dir, "c.txt")
            nested_dir = os.path.join(temp_dir, "nested")
            os.makedirs(nested_dir, exist_ok=True)
            nested_pdf = os.path.join(nested_dir, "d.pdf")

            for path in (pdf_path, image_path, ignored_path, nested_pdf):
                with open(path, "wb") as file_obj:
                    file_obj.write(b"data")

            files = quick_send.collect_supported_input_files([temp_dir, pdf_path])

            self.assertEqual(
                files,
                [
                    os.path.abspath(pdf_path),
                    os.path.abspath(image_path),
                    os.path.abspath(nested_pdf),
                ],
            )

    def test_collect_supported_push_input_files_includes_zip(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = os.path.join(temp_dir, "a.pdf")
            zip_path = os.path.join(temp_dir, "pack.zip")
            with open(pdf_path, "wb") as file_obj:
                file_obj.write(b"%PDF-1.4")
            with zipfile.ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("inner.pdf", b"%PDF-1.4")

            files = quick_send.collect_supported_push_input_files([temp_dir])

            self.assertEqual(
                files,
                [
                    os.path.abspath(pdf_path),
                    os.path.abspath(zip_path),
                ],
            )

    def test_build_webhook_upload_url_uses_same_host(self):
        upload_url = quick_send.build_webhook_upload_url(
            "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-key"
        )
        self.assertEqual(
            upload_url,
            "https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key=test-key&type=file",
        )

    def test_build_batch_helpers_use_manual_cutoff_date(self):
        self.assertEqual(quick_send.normalize_batch_cutoff_date("2026-04-22"), "260422")
        self.assertEqual(quick_send.normalize_batch_cutoff_date("2026-04-22 17:30"), "2604221730")
        self.assertEqual(quick_send.normalize_batch_cutoff_date("260422 17.30"), "2604221730")
        self.assertEqual(quick_send.format_batch_cutoff_date("260422"), "2026-04-22")
        self.assertEqual(quick_send.format_batch_cutoff_date("2604221730"), "2026-04-22 17:30")
        self.assertEqual(quick_send.build_batch_zip_name("260422"), "发票截止2026-04-22.zip")
        self.assertEqual(quick_send.build_batch_zip_name("2604221730"), "发票截止2026-04-22_17-30.zip")
        self.assertEqual(
            quick_send.build_batch_summary_text(12, "260422"),
            "本轮发票已推送完成，共12张，截止到2026-04-22以及之前的发票。"
            "可以通过搜索发票抬头或者发票金额查询相应发票，"
            "店长上传发票可以下载压缩包到本地搜索相应发票上传。",
        )
        self.assertEqual(
            quick_send.build_batch_summary_text(12, "2604221730"),
            "本轮发票已推送完成，共12张，截止到2026-04-22 17:30以及之前的发票。"
            "可以通过搜索发票抬头或者发票金额查询相应发票，"
            "店长上传发票可以下载压缩包到本地搜索相应发票上传。",
        )
        self.assertEqual(
            quick_send.build_batch_summary_text(
                12,
                "260422",
                template="截止{cutoff_date}，共{count}张，请查收。",
            ),
            "截止2026-04-22，共12张，请查收。",
        )

    def test_send_file_via_wecom_webhook_uploads_then_sends_message(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "invoice.pdf")
            with open(file_path, "wb") as file_obj:
                file_obj.write(b"%PDF-1.4")

            session = FakeSession()
            result = quick_send.send_file_via_wecom_webhook(
                "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-key",
                file_path,
                session=session,
            )

            self.assertEqual(result["errcode"], 0)
            self.assertEqual(len(session.calls), 2)
            self.assertEqual(
                session.calls[0]["url"],
                "https://qyapi.weixin.qq.com/cgi-bin/webhook/upload_media?key=test-key&type=file",
            )
            self.assertEqual(session.calls[1]["json"], {"msgtype": "file", "file": {"media_id": "media-123"}})

    def test_send_file_via_wecom_webhook_requires_key(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            file_path = os.path.join(temp_dir, "invoice.pdf")
            with open(file_path, "wb") as file_obj:
                file_obj.write(b"%PDF-1.4")

            with self.assertRaises(ValueError):
                quick_send.send_file_via_wecom_webhook(
                    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send",
                    file_path,
                    session=FakeSession(),
                )

    def test_send_file_via_wecom_webhook_with_retry_retries_on_rate_limit(self):
        with mock.patch.object(
            quick_send,
            "send_file_via_wecom_webhook",
            side_effect=[
                quick_send.WeComRateLimitError("limit"),
                quick_send.WeComRateLimitError("limit"),
                {"errcode": 0},
            ],
        ) as send_file:
            with mock.patch.object(quick_send.time, "sleep") as sleep_mock:
                retries_used = quick_send.send_file_via_wecom_webhook_with_retry(
                    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-key",
                    "invoice.pdf",
                    logger=lambda *_args, **_kwargs: None,
                )

        self.assertEqual(retries_used, 2)
        self.assertEqual(send_file.call_count, 3)
        self.assertEqual(
            sleep_mock.call_args_list,
            [
                mock.call(quick_send.WECOM_RATE_LIMIT_BACKOFF_SECONDS),
                mock.call(quick_send.WECOM_RATE_LIMIT_BACKOFF_SECONDS),
            ],
        )

    def test_process_inputs_and_send_waits_configured_seconds_between_pushes(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            first_pdf = os.path.join(temp_dir, "invoice-1.pdf")
            second_pdf = os.path.join(temp_dir, "invoice-2.pdf")
            for path in (first_pdf, second_pdf):
                with open(path, "wb") as file_obj:
                    file_obj.write(b"%PDF-1.4")

            generated_paths = [
                os.path.join(temp_dir, "renamed-1.pdf"),
                os.path.join(temp_dir, "renamed-2.pdf"),
            ]
            batch_zip_path = os.path.join(temp_dir, "发票截止260422.zip")
            with open(batch_zip_path, "wb") as file_obj:
                file_obj.write(b"zip")

            with mock.patch.object(quick_send.main, "copy_pdf_with_generated_name") as copy_pdf:
                with mock.patch.object(
                    quick_send,
                    "send_file_via_wecom_webhook_with_retry",
                    return_value=0,
                ) as send_file:
                    with mock.patch.object(
                        quick_send,
                        "send_text_via_wecom_webhook_with_retry",
                        return_value=0,
                    ) as send_text:
                        with mock.patch.object(
                            quick_send,
                            "create_batch_pdf_zip",
                            return_value=batch_zip_path,
                        ):
                            with mock.patch.object(quick_send.time, "sleep") as sleep_mock:
                                copy_pdf.side_effect = [
                                    {"file_path": generated_paths[0], "file_name": "renamed-1.pdf"},
                                    {"file_path": generated_paths[1], "file_name": "renamed-2.pdf"},
                                ]

                                summary = quick_send.process_inputs_and_send(
                                    [first_pdf, second_pdf],
                                    temp_dir,
                                    "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-key",
                                    logger=lambda *_args, **_kwargs: None,
                                    send_interval_seconds=1.5,
                                )

            self.assertEqual(summary["sent"], 2)
            self.assertEqual(send_file.call_count, 3)
            self.assertTrue(summary["batch_zip_sent"])
            self.assertTrue(summary["summary_text_sent"])
            self.assertEqual(send_text.call_count, 1)
            sleep_mock.assert_called_once_with(1.5)

    def test_process_inputs_locally_handles_pdf_and_qr(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = os.path.join(temp_dir, "invoice.pdf")
            image_path = os.path.join(temp_dir, "code.png")
            with open(pdf_path, "wb") as file_obj:
                file_obj.write(b"%PDF-1.4")
            with open(image_path, "wb") as file_obj:
                file_obj.write(b"fake-image")

            with mock.patch.object(quick_send.main, "configure_paths") as configure_paths:
                with mock.patch.object(quick_send.main, "load_processed_index", return_value={}):
                    with mock.patch.object(quick_send.main, "copy_pdf_with_generated_name") as copy_pdf:
                        with mock.patch.object(quick_send.main, "decode_qr_image", return_value="https://example.com?cs=demo-cs"):
                            with mock.patch.object(quick_send.main, "extract_cs_from_url", return_value="demo-cs"):
                                with mock.patch.object(quick_send.main, "download_and_rename_to_output") as download_qr:
                                    with mock.patch.object(quick_send.main, "save_processed_index") as save_index:
                                        with mock.patch.object(quick_send.main, "write_failed_log") as write_failed_log:
                                            copy_pdf.return_value = {
                                                "file_path": os.path.join(temp_dir, "pdf-result.pdf"),
                                                "file_name": "pdf-result.pdf",
                                            }
                                            download_qr.return_value = {
                                                "status": "success",
                                                "cs": "demo-cs",
                                                "invoice_number": "123456",
                                                "file_path": os.path.join(temp_dir, "qr-result.pdf"),
                                                "file_name": "qr-result.pdf",
                                            }

                                            summary = quick_send.process_inputs_locally(
                                                [pdf_path, image_path],
                                                temp_dir,
                                                logger=lambda *_args, **_kwargs: None,
                                            )

            self.assertEqual(summary["mode"], "local")
            self.assertEqual(summary["total"], 2)
            self.assertEqual(summary["pdf_inputs"], 1)
            self.assertEqual(summary["image_inputs"], 1)
            self.assertEqual(summary["success"], 2)
            self.assertEqual(summary["skipped"], 0)
            self.assertEqual(summary["failed"], 0)
            configure_paths.assert_called_once_with(output_dir=os.path.abspath(temp_dir))
            save_index.assert_called_once()
            write_failed_log.assert_called_once_with([])

    def test_process_inputs_locally_skips_processed_qr(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            image_path = os.path.join(temp_dir, "code.png")
            with open(image_path, "wb") as file_obj:
                file_obj.write(b"fake-image")

            with mock.patch.object(quick_send.main, "configure_paths"):
                with mock.patch.object(
                    quick_send.main,
                    "load_processed_index",
                    return_value={"demo-cs": {"file_name": "existing.pdf"}},
                ):
                    with mock.patch.object(quick_send.main, "decode_qr_image", return_value="https://example.com?cs=demo-cs"):
                        with mock.patch.object(quick_send.main, "extract_cs_from_url", return_value="demo-cs"):
                            with mock.patch.object(quick_send.main, "download_and_rename_to_output") as download_qr:
                                with mock.patch.object(quick_send.main, "write_failed_log") as write_failed_log:
                                    summary = quick_send.process_inputs_locally(
                                        [image_path],
                                        temp_dir,
                                        logger=lambda *_args, **_kwargs: None,
                                    )

            self.assertEqual(summary["success"], 0)
            self.assertEqual(summary["skipped"], 1)
            self.assertEqual(summary["failed"], 0)
            download_qr.assert_not_called()
            write_failed_log.assert_called_once_with([])

    def test_process_inputs_and_send_extracts_zip_and_sends_inner_pdf(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = os.path.join(temp_dir, "pack.zip")
            with zipfile.ZipFile(zip_path, "w") as zip_file:
                zip_file.writestr("nested/invoice.pdf", b"%PDF-1.4")
            batch_zip_path = os.path.join(temp_dir, "发票截止260422.zip")
            with open(batch_zip_path, "wb") as file_obj:
                file_obj.write(b"zip")

            with mock.patch.object(quick_send.main, "copy_pdf_with_generated_name") as copy_pdf:
                with mock.patch.object(
                    quick_send,
                    "send_file_via_wecom_webhook_with_retry",
                    return_value=0,
                ) as send_file:
                    with mock.patch.object(
                        quick_send,
                        "send_text_via_wecom_webhook_with_retry",
                        return_value=0,
                    ) as send_text:
                        with mock.patch.object(
                            quick_send,
                            "create_batch_pdf_zip",
                            return_value=batch_zip_path,
                        ):
                            copy_pdf.return_value = {
                                "file_path": os.path.join(temp_dir, "renamed.pdf"),
                                "file_name": "renamed.pdf",
                            }

                            summary = quick_send.process_inputs_and_send(
                                [zip_path],
                                temp_dir,
                                "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-key",
                                logger=lambda *_args, **_kwargs: None,
                            )

            self.assertEqual(summary["mode"], "push")
            self.assertEqual(summary["total"], 1)
            self.assertEqual(summary["pdf_inputs"], 1)
            self.assertEqual(summary["image_inputs"], 0)
            self.assertEqual(summary["sent"], 1)
            self.assertEqual(summary["failed"], 0)
            self.assertTrue(summary["batch_zip_sent"])
            self.assertTrue(summary["summary_text_sent"])
            copy_pdf.assert_called_once()
            sent_path = send_file.call_args_list[0][0][1]
            self.assertEqual(os.path.basename(sent_path), "renamed.pdf")
            self.assertEqual(send_text.call_count, 1)

    def test_process_inputs_and_send_skips_duplicate_invoice_numbers(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            first_pdf = os.path.join(temp_dir, "invoice-1.pdf")
            second_pdf = os.path.join(temp_dir, "invoice-2.pdf")
            for path in (first_pdf, second_pdf):
                with open(path, "wb") as file_obj:
                    file_obj.write(b"%PDF-1.4")

            first_renamed = os.path.join(temp_dir, "same-title-1.pdf")
            second_renamed = os.path.join(temp_dir, "same-title-2.pdf")
            batch_zip_path = os.path.join(temp_dir, "发票截止2026-04-22.zip")
            for path in (first_renamed, second_renamed, batch_zip_path):
                with open(path, "wb") as file_obj:
                    file_obj.write(b"%PDF-1.4")

            with mock.patch.object(quick_send.main, "copy_pdf_with_generated_name") as copy_pdf:
                with mock.patch.object(
                    quick_send,
                    "send_file_via_wecom_webhook_with_retry",
                    return_value=0,
                ) as send_file:
                    with mock.patch.object(
                        quick_send,
                        "send_text_via_wecom_webhook_with_retry",
                        return_value=0,
                    ):
                        with mock.patch.object(
                            quick_send,
                            "create_batch_pdf_zip",
                            return_value=batch_zip_path,
                        ):
                            copy_pdf.side_effect = [
                                {
                                    "file_path": first_renamed,
                                    "file_name": os.path.basename(first_renamed),
                                    "invoice_number": "12345678",
                                },
                                {
                                    "file_path": second_renamed,
                                    "file_name": os.path.basename(second_renamed),
                                    "invoice_number": "12345678",
                                },
                            ]

                            summary = quick_send.process_inputs_and_send(
                                [first_pdf, second_pdf],
                                temp_dir,
                                "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-key",
                                logger=lambda *_args, **_kwargs: None,
                            )

            self.assertEqual(summary["sent"], 1)
            self.assertEqual(summary["skipped_duplicates"], 1)
            self.assertEqual(send_file.call_count, 2)

    def test_process_inputs_and_send_resends_failed_pdf_at_end(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = os.path.join(temp_dir, "invoice.pdf")
            with open(pdf_path, "wb") as file_obj:
                file_obj.write(b"%PDF-1.4")

            with mock.patch.object(quick_send.main, "copy_pdf_with_generated_name") as copy_pdf:
                with mock.patch.object(
                    quick_send,
                    "send_file_via_wecom_webhook_with_retry",
                    side_effect=[ValueError("first send failed"), 1, 0],
                ) as send_file:
                    with mock.patch.object(
                        quick_send,
                        "send_text_via_wecom_webhook_with_retry",
                        return_value=0,
                    ):
                        renamed_path = os.path.join(temp_dir, "renamed.pdf")
                        with open(renamed_path, "wb") as file_obj:
                            file_obj.write(b"%PDF-1.4")
                        copy_pdf.return_value = {
                            "file_path": renamed_path,
                            "file_name": "renamed.pdf",
                        }

                        summary = quick_send.process_inputs_and_send(
                            [pdf_path],
                            temp_dir,
                            "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-key",
                            logger=lambda *_args, **_kwargs: None,
                        )

            self.assertEqual(send_file.call_count, 3)
            self.assertEqual(summary["sent"], 1)
            self.assertEqual(summary["failed"], 0)
            self.assertEqual(summary["resend_queued"], 1)
            self.assertEqual(summary["resend_sent"], 1)
            self.assertEqual(summary["resend_failed"], 0)
            self.assertTrue(summary["batch_zip_sent"])
            self.assertTrue(summary["summary_text_sent"])

    def test_process_inputs_and_send_writes_push_failed_log(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            pdf_path = os.path.join(temp_dir, "invoice.pdf")
            with open(pdf_path, "wb") as file_obj:
                file_obj.write(b"%PDF-1.4")

            with mock.patch.object(quick_send.main, "copy_pdf_with_generated_name") as copy_pdf:
                with mock.patch.object(
                    quick_send,
                    "send_file_via_wecom_webhook_with_retry",
                    side_effect=[ValueError("send failed"), ValueError("send failed again")],
                ):
                    renamed_path = os.path.join(temp_dir, "renamed.pdf")
                    with open(renamed_path, "wb") as file_obj:
                        file_obj.write(b"%PDF-1.4")
                    copy_pdf.return_value = {
                        "file_path": renamed_path,
                        "file_name": "renamed.pdf",
                    }

                    summary = quick_send.process_inputs_and_send(
                        [pdf_path],
                        temp_dir,
                        "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test-key",
                        logger=lambda *_args, **_kwargs: None,
                    )

            self.assertEqual(summary["sent"], 0)
            self.assertEqual(summary["failed"], 1)
            self.assertEqual(summary["resend_failed"], 1)
            self.assertEqual(len(summary["manual_retry_files"]), 1)
            self.assertTrue(os.path.exists(summary["manual_retry_files"][0]))
            self.assertTrue(os.path.isdir(summary["manual_retry_dir"]))
            log_path = os.path.join(temp_dir, quick_send.PUSH_FAILED_LOG_NAME)
            self.assertTrue(os.path.exists(log_path))
            with open(log_path, "r", encoding="utf-8") as file_obj:
                log_text = file_obj.read()
            self.assertIn("send_wecom", log_text)
            self.assertIn(os.path.abspath(pdf_path), log_text)
            self.assertIn("send failed again", log_text)


if __name__ == "__main__":
    unittest.main()
