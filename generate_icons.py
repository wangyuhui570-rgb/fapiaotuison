from pathlib import Path

from PIL import Image, ImageDraw


ROOT = Path(__file__).resolve().parent
ASSETS_DIR = ROOT / "assets"
ICONS_DIR = ASSETS_DIR / "icons"

ACCENT = "#0A84FF"
ACCENT_SOFT = "#E8F3FF"
TEXT = "#253041"
MUTED = "#748092"
WHITE = "#FFFFFF"

ICON_SIZE = 24
STROKE = 2


def ensure_dirs():
    ASSETS_DIR.mkdir(exist_ok=True)
    ICONS_DIR.mkdir(exist_ok=True)


def save_app_icon():
    size = 256
    image = Image.new("RGBA", (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)

    draw.rounded_rectangle((10, 10, size - 10, size - 10), radius=60, fill=ACCENT)
    draw.rounded_rectangle((24, 24, size - 24, size - 24), radius=48, fill="#0E76E8")

    doc = (56, 44, 178, 206)
    draw.rounded_rectangle(doc, radius=26, fill=WHITE)
    draw.polygon([(146, 44), (178, 44), (178, 78)], fill="#D7EAFF")

    arrow_box = (138, 134, 212, 208)
    draw.rounded_rectangle(arrow_box, radius=22, fill=ACCENT_SOFT)
    draw.line((175, 150, 175, 182), fill=ACCENT, width=10)
    draw.polygon([(156, 174), (175, 193), (194, 174)], fill=ACCENT)

    for y in (86, 112, 138):
        draw.rounded_rectangle((82, y, 138, y + 10), radius=5, fill="#CFE3FF")

    png_path = ASSETS_DIR / "app_icon.png"
    ico_path = ASSETS_DIR / "app_icon.ico"
    image.save(png_path)
    image.save(ico_path, sizes=[(256, 256), (128, 128), (64, 64), (48, 48), (32, 32), (16, 16)])


def draw_icon(name, painter):
    image = Image.new("RGBA", (ICON_SIZE, ICON_SIZE), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    painter(draw)
    image.save(ICONS_DIR / f"{name}.png")


def paint_download(draw):
    draw.line((12, 5, 12, 14), fill=TEXT, width=STROKE)
    draw.polygon([(8, 12), (12, 17), (16, 12)], fill=TEXT)
    draw.line((7, 19, 17, 19), fill=MUTED, width=STROKE)


def paint_log(draw):
    draw.rounded_rectangle((5, 5, 19, 19), radius=5, outline=MUTED, width=1)
    for y in (9, 12, 15):
        draw.line((8, y, 16, y), fill=TEXT if y == 9 else MUTED, width=STROKE)


def paint_collapse(draw):
    draw.line((8, 10, 12, 14), fill=TEXT, width=STROKE)
    draw.line((16, 10, 12, 14), fill=TEXT, width=STROKE)


def paint_more(draw):
    for x in (7, 12, 17):
        draw.ellipse((x - 1, 11 - 1, x + 1, 11 + 1), fill=TEXT)


def paint_preview(draw):
    draw.rounded_rectangle((4, 7, 20, 17), radius=5, outline=MUTED, width=STROKE)
    draw.ellipse((10, 10, 14, 14), outline=TEXT, width=STROKE)
    draw.line((14, 14, 17, 17), fill=TEXT, width=STROKE)


def paint_cleanup(draw):
    draw.line((7, 8, 17, 8), fill=MUTED, width=STROKE)
    draw.line((10, 6, 14, 6), fill=MUTED, width=STROKE)
    draw.rounded_rectangle((8, 8, 16, 19), radius=2, outline=MUTED, width=STROKE)
    for x in (10, 12, 14):
        draw.line((x, 11, x, 16), fill=TEXT, width=1)


def paint_folder(draw):
    draw.line((4, 10, 10, 10), fill=MUTED, width=STROKE)
    draw.line((10, 10, 12, 8), fill=MUTED, width=STROKE)
    draw.line((12, 8, 18, 8), fill=MUTED, width=STROKE)
    draw.rounded_rectangle((4, 9, 20, 18), radius=4, outline=MUTED, width=STROKE)


def paint_help(draw):
    draw.ellipse((4, 4, 20, 20), outline=MUTED, width=STROKE)
    draw.arc((8, 7, 16, 13), start=180, end=360, fill=TEXT, width=STROKE)
    draw.line((12, 13, 12, 15), fill=TEXT, width=STROKE)
    draw.ellipse((11, 18, 13, 20), fill=MUTED)


def paint_exit(draw):
    draw.line((8, 8, 16, 16), fill=TEXT, width=STROKE)
    draw.line((16, 8, 8, 16), fill=TEXT, width=STROKE)


def paint_clear(draw):
    draw.line((8, 8, 16, 16), fill=MUTED, width=STROKE)
    draw.line((16, 8, 8, 16), fill=MUTED, width=STROKE)


def main():
    ensure_dirs()
    save_app_icon()
    draw_icon("download", paint_download)
    draw_icon("log", paint_log)
    draw_icon("collapse", paint_collapse)
    draw_icon("more", paint_more)
    draw_icon("preview", paint_preview)
    draw_icon("cleanup", paint_cleanup)
    draw_icon("folder", paint_folder)
    draw_icon("help", paint_help)
    draw_icon("exit", paint_exit)
    draw_icon("clear", paint_clear)


if __name__ == "__main__":
    main()
