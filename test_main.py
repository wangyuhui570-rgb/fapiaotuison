import os
import shutil
import unittest
from unittest import mock
from pathlib import Path

import invoice_desktop
import main


class BuildExportParamsTests(unittest.TestCase):
    def test_extracts_variable_length_check_code(self):
        cases = {
            "2_26322000002973604036_20260417123304001XH8B9A": ("26322000002973604036", "20260417123304", "8B9A"),
            "2_26322000002971860886_20260417123933801XH225": ("26322000002971860886", "20260417123933", "225"),
            "2_26322000002978375776_20260417151400001XHBAA": ("26322000002978375776", "20260417151400", "BAA"),
            "2_26322000002980735081_20260417152217001XH1C1": ("26322000002980735081", "20260417152217", "1C1"),
        }

        for cs_value, expected in cases.items():
            with self.subTest(cs_value=cs_value):
                params = main.build_export_params(cs_value)
                self.assertEqual(params["Fphm"], expected[0])
                self.assertEqual(params["Kprq"], expected[1])
                self.assertEqual(params["Jym"], expected[2])
                self.assertEqual(params["Wjgs"], "PDF")
                self.assertTrue(params["Czsj"].isdigit())


class OutputStemTests(unittest.TestCase):
    def test_prefers_buyer_and_amount(self):
        result = main.choose_output_stem("测试公司", "88.00", "img001", "123456")
        self.assertEqual(result, "测试公司-88.00")

    def test_falls_back_to_invoice_number_when_buyer_only(self):
        result = main.choose_output_stem("测试/公司", None, "img001", "123456")
        self.assertEqual(result, "测试公司-123456")

    def test_falls_back_to_image_name_when_amount_only(self):
        result = main.choose_output_stem(None, "88.00", "img001", "123456")
        self.assertEqual(result, "img001-88.00")


class DuplicateCleanupTests(unittest.TestCase):
    def test_choose_duplicate_keeper_prefers_non_timestamp_name(self):
        names = ["发票A_1776421237.pdf", "发票A.pdf", "发票A_1776429999.pdf"]
        self.assertEqual(main.choose_duplicate_keeper(names), "发票A.pdf")

    def test_find_duplicate_pdfs_prefers_index_name(self):
        keep_name = "keep.pdf"
        remove_name = "dup.pdf"
        processed_index = {
            "cs-demo": {
                "file_name": keep_name,
                "invoice_number": "123",
            }
        }

        with mock.patch.object(main, "list_pdf_files", return_value=[keep_name, remove_name]):
            with mock.patch.object(main, "file_sha256", return_value="same-hash"):
                duplicates = main.find_duplicate_pdfs("unused-dir", processed_index)

        self.assertEqual(len(duplicates), 1)
        self.assertEqual(duplicates[0]["keep"], keep_name)
        self.assertEqual(duplicates[0]["remove"], [remove_name])


class ClearDirectoryTests(unittest.TestCase):
    def test_clear_directory_contents_removes_files_and_subdirectories(self):
        temp_dir = os.path.join(os.getcwd(), "_clear_test_dir")
        shutil.rmtree(temp_dir, ignore_errors=True)
        os.makedirs(temp_dir, exist_ok=True)
        try:
            base = Path(temp_dir)
            (base / "q1.png").write_text("a", encoding="utf-8")
            nested = base / "nested"
            nested.mkdir()
            (nested / "q2.jpg").write_text("b", encoding="utf-8")

            result = main.clear_directory_contents(temp_dir)

            self.assertEqual(result["removed_files"], 2)
            self.assertEqual(result["removed_dirs"], 1)
            self.assertTrue(base.exists())
            self.assertEqual(list(base.iterdir()), [])
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class DropImportTests(unittest.TestCase):
    def test_copy_images_to_directory_filters_and_renames(self):
        source_dir = os.path.join(os.getcwd(), "_drop_source_dir")
        target_dir = os.path.join(os.getcwd(), "_drop_target_dir")
        shutil.rmtree(source_dir, ignore_errors=True)
        shutil.rmtree(target_dir, ignore_errors=True)
        os.makedirs(source_dir, exist_ok=True)
        os.makedirs(target_dir, exist_ok=True)

        try:
            first_image = os.path.join(source_dir, "code.png")
            second_image = os.path.join(source_dir, "code.jpg")
            ignored_file = os.path.join(source_dir, "note.txt")
            Path(first_image).write_text("img1", encoding="utf-8")
            Path(second_image).write_text("img2", encoding="utf-8")
            Path(ignored_file).write_text("skip", encoding="utf-8")
            Path(os.path.join(target_dir, "code.png")).write_text("existing", encoding="utf-8")

            copied = invoice_desktop.copy_images_to_directory(
                [first_image, second_image, ignored_file],
                target_dir,
            )

            copied_names = sorted(Path(path).name for path in copied)
            self.assertEqual(len(copied_names), 2)
            self.assertIn("code.jpg", copied_names)
            self.assertTrue(any(name.startswith("code_") and name.endswith(".png") for name in copied_names))
        finally:
            shutil.rmtree(source_dir, ignore_errors=True)
            shutil.rmtree(target_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
