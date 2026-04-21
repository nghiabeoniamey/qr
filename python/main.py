import argparse
import io
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from PIL import Image
from reportlab.lib.pagesizes import A4
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


def fetch_marker(
    base_url: str,
    class_id: int,
    marker_id: int,
    size: int,
    *,
    timeout: float,
    retries: int,
) -> bytes:
    query = urlencode({"classId": class_id, "id": marker_id, "size": size})
    url = f"{base_url}?{query}"
    last_err: BaseException | None = None
    attempts = max(1, retries + 1)
    for attempt in range(attempts):
        try:
            with urlopen(url, timeout=timeout) as response:
                return response.read()
        except (URLError, HTTPError, TimeoutError, OSError) as exc:
            last_err = exc
            if attempt < attempts - 1:
                time.sleep(min(2**attempt, 8))
    hint = (
        "Không kết nối được tới API. Kiểm tra: cùng mạng LAN với máy chủ, "
        "dịch vụ đang chạy (port 7070), firewall/VPN, và đúng --base-url."
    )
    raise RuntimeError(
        f"Tải marker thất bại (id={marker_id}).\n"
        f"URL: {url}\n"
        f"Lỗi: {last_err!r}\n"
        f"{hint}"
    ) from last_err


def trim_whitespace(image_bytes: bytes) -> Image.Image:
    image = Image.open(io.BytesIO(image_bytes)).convert("RGB")
    # Trim white margin so marker content appears larger in the same cell.
    gray = image.convert("L")
    bbox = gray.point(lambda p: 255 if p < 245 else 0).getbbox()
    if bbox:
        image = image.crop(bbox)
    return image


def build_pdf(
    output_path: str,
    base_url: str,
    class_id: int,
    start_id: int,
    count: int,
    size: int,
    zoom: float,
    per_page: int,
    timeout: float,
    retries: int,
) -> None:
    page_width, page_height = A4
    margin = 28
    gap = 18
    title_space = 30

    if per_page == 1:
        cols, rows = 1, 1
    elif per_page == 2:
        cols, rows = 1, 2
    else:
        cols, rows = 2, 2

    cell_w = (page_width - 2 * margin - (cols - 1) * gap) / cols
    cell_h = (page_height - 2 * margin - title_space - (rows - 1) * gap) / rows

    pdf = canvas.Canvas(output_path, pagesize=A4)
    marker_ids = [start_id + i for i in range(count)]

    for page_start in range(0, len(marker_ids), per_page):
        page_ids = marker_ids[page_start : page_start + per_page]
        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawCentredString(
            page_width / 2,
            page_height - margin + 8,
            f"ArUco Markers (Class {class_id}) - {per_page} per page",
        )

        for idx, marker_id in enumerate(page_ids):
            row = idx // cols
            col = idx % cols
            x = margin + col * (cell_w + gap)
            y = page_height - margin - title_space - (row + 1) * cell_h - row * gap

            image_bytes = fetch_marker(
                base_url,
                class_id,
                marker_id,
                size,
                timeout=timeout,
                retries=retries,
            )
            pil_img = trim_whitespace(image_bytes)
            image = ImageReader(pil_img)
            img_w, img_h = image.getSize()

            max_w = cell_w - 16
            max_h = cell_h - 40
            scale = min(max_w / img_w, max_h / img_h)
            scale *= zoom
            draw_w = img_w * scale
            draw_h = img_h * scale
            if draw_w > max_w or draw_h > max_h:
                cap = min(max_w / draw_w, max_h / draw_h)
                draw_w *= cap
                draw_h *= cap
            draw_x = x + (cell_w - draw_w) / 2
            draw_y = y + (cell_h - draw_h) / 2 + 10

            pdf.drawImage(
                image,
                draw_x,
                draw_y,
                width=draw_w,
                height=draw_h,
                preserveAspectRatio=True,
                mask="auto",
            )
            pdf.setFont("Helvetica-Bold", 12)
            pdf.drawCentredString(x + cell_w / 2, y + 8, f"id = {marker_id}")

        pdf.showPage()

    pdf.save()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate ArUco marker PDF (1/2/4 markers per A4)."
    )
    parser.add_argument(
        "--base-url",
        default="http://192.168.1.251:7070/api/qrcodes/generate-aruco-marker",
        help="API endpoint for marker generation.",
    )
    parser.add_argument("--class-id", type=int, default=992, help="Aruco classId value.")
    parser.add_argument("--start-id", type=int, default=260, help="Starting marker id.")
    parser.add_argument("--count", type=int, default=4, help="Total markers to generate.")
    parser.add_argument("--size", type=int, default=400, help="Marker image size from API.")
    parser.add_argument(
        "--zoom",
        type=float,
        default=2.0,
        help="Visual zoom factor inside each cell (default: 2.0).",
    )
    parser.add_argument(
        "--output", default="aruco_4up.pdf", help="Output PDF path, e.g. aruco_4up.pdf"
    )
    parser.add_argument(
        "--per-page",
        type=int,
        choices=[1, 2, 4],
        default=4,
        help="Number of markers per A4 page: 1, 2, or 4.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP timeout in seconds per request (default: 60).",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Extra retries on network errors (default: 3).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_pdf(
        output_path=args.output,
        base_url=args.base_url,
        class_id=args.class_id,
        start_id=args.start_id,
        count=args.count,
        size=args.size,
        zoom=args.zoom,
        per_page=args.per_page,
        timeout=args.timeout,
        retries=args.retries,
    )
