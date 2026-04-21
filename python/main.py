import argparse
import io
import json
import os
import secrets
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from PIL import Image
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.lib.utils import ImageReader
from reportlab.pdfgen import canvas


def load_env_file(path: Path, *, override: bool = False) -> None:
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip()
        if len(val) >= 2 and val[0] == val[-1] and val[0] in "\"'":
            val = val[1:-1]
        if not key:
            continue
        if not override and key in os.environ:
            continue
        os.environ[key] = val


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


def _http_get_json(
    url: str,
    *,
    headers: dict[str, str],
    timeout: float,
    retries: int,
) -> dict:
    last_err: BaseException | None = None
    attempts = max(1, retries + 1)
    for attempt in range(attempts):
        try:
            req = Request(url, headers=headers, method="GET")
            with urlopen(req, timeout=timeout) as response:
                body = response.read().decode("utf-8")
            return json.loads(body)
        except (URLError, HTTPError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            last_err = exc
            if attempt < attempts - 1:
                time.sleep(min(2**attempt, 8))
    raise RuntimeError(f"GET thất bại: {url}\nLỗi: {last_err!r}") from last_err


def _encode_multipart_form(fields: dict[str, str]) -> tuple[str, bytes]:
    boundary = f"----PyFormBoundary{secrets.token_hex(16)}"
    chunks: list[bytes] = []
    for name, value in fields.items():
        chunks.append(f"--{boundary}\r\n".encode("ascii"))
        chunks.append(
            f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode("utf-8")
        )
        chunks.append(value.encode("utf-8"))
        chunks.append(b"\r\n")
    chunks.append(f"--{boundary}--\r\n".encode("ascii"))
    body = b"".join(chunks)
    content_type = f"multipart/form-data; boundary={boundary}"
    return content_type, body


def _http_post_json(
    url: str,
    *,
    body: bytes,
    headers: dict[str, str],
    timeout: float,
    retries: int,
) -> dict:
    merged = {"Accept": "application/json", **headers}
    last_err: BaseException | None = None
    attempts = max(1, retries + 1)
    for attempt in range(attempts):
        try:
            req = Request(url, data=body, headers=merged, method="POST")
            with urlopen(req, timeout=timeout) as response:
                text = response.read().decode("utf-8")
            return json.loads(text)
        except HTTPError as exc:
            try:
                detail = exc.read().decode("utf-8", errors="replace")
            except Exception:
                detail = ""
            raise RuntimeError(
                f"POST thất bại: {url}\nHTTP {exc.code}\n{detail}"
            ) from exc
        except (URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
            last_err = exc
            if attempt < attempts - 1:
                time.sleep(min(2**attempt, 8))
    raise RuntimeError(f"POST thất bại: {url}\nLỗi: {last_err!r}") from last_err


def login_fetch_id_token(
    *,
    login_url: str,
    username: str,
    password: str,
    timeout: float,
    retries: int,
) -> str:
    content_type, body = _encode_multipart_form(
        {"username": username, "password": password}
    )
    payload = _http_post_json(
        login_url,
        body=body,
        headers={"Content-Type": content_type},
        timeout=timeout,
        retries=retries,
    )
    if not payload.get("result"):
        raise RuntimeError(f"Đăng nhập trả về result=false: {payload!r}")
    data = payload.get("data") or {}
    token = data.get("id_token") or data.get("access_token") or data.get("token")
    if not token or not str(token).strip():
        raise RuntimeError(f"Đăng nhập không có id_token trong data: {payload!r}")
    return str(token).strip()


def fetch_pupil_id_to_name(
    *,
    list_pupils_url: str,
    bearer_token: str,
    timeout: float,
    retries: int,
) -> dict[int, str]:
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {bearer_token.strip()}",
    }
    payload = _http_get_json(
        list_pupils_url, headers=headers, timeout=timeout, retries=retries
    )
    if not payload.get("result"):
        raise RuntimeError(f"list-pupils trả về result=false: {payload!r}")
    out: dict[int, str] = {}
    for row in payload.get("data") or []:
        pid = row.get("id")
        if pid is None:
            continue
        name = (row.get("name") or "").strip()
        if not name:
            continue
        key = int(pid)
        if key not in out:
            out[key] = name
    return out


def _string_width(text: str, font: str, size: float) -> float:
    try:
        return pdfmetrics.stringWidth(text, font, size)
    except Exception:
        return pdfmetrics.stringWidth(text, "Helvetica", size)


def _wrap_label_lines(
    text: str, font: str, size: float, max_width: float, max_lines: int = 3
) -> list[str]:
    text = (text or "").strip()
    if not text:
        return []
    words = text.split()
    lines: list[str] = []
    current: list[str] = []
    for w in words:
        trial = " ".join(current + [w])
        if _string_width(trial, font, size) <= max_width:
            current.append(w)
            continue
        if current:
            lines.append(" ".join(current))
            current = [w]
        else:
            lines.append(w)
            current = []
        if len(lines) >= max_lines:
            current = []
            break
    if current and len(lines) < max_lines:
        lines.append(" ".join(current))
    lines = lines[:max_lines]
    for i, line in enumerate(lines):
        while _string_width(line, font, size) > max_width and len(line) > 1:
            line = line[:-1].rstrip() + "…"
            lines[i] = line
    return lines


def draw_label_below_cell(
    pdf: canvas.Canvas,
    *,
    cx: float,
    y_bottom: float,
    text: str,
    max_width: float,
    font: str = "Helvetica-Bold",
    base_size: float = 11.0,
) -> None:
    lines = _wrap_label_lines(text, font, base_size, max_width, max_lines=3)
    if not lines:
        return
    lh = base_size + 2
    py = y_bottom
    pdf.setFont(font, base_size)
    for line in lines:
        pdf.drawCentredString(cx, py, line)
        py += lh


def draw_orientation_letters_around_qr(
    pdf: canvas.Canvas,
    *,
    left: float,
    bottom: float,
    width: float,
    height: float,
    font: str = "Helvetica-Bold",
    size: float = 32,
    extra_edge_pt: float = 0.0,
) -> None:
    """B trên, A phải, C trái, D dưới QR; chân chữ hướng ra ngoài (mép tờ / mép ô)."""
    cx = left + width / 2
    cy = bottom + height / 2
    # Khoảng từ mép bbox ảnh marker tới chữ (gấp đôi bản cũ) — A/B/C/D nằm xa mép marker
    cap_h = size * 0.74
    base_edge = max(28.0, 16.0 + size * 0.52) + extra_edge_pt
    edge_clear = base_edge * 2.0
    top_edge = bottom + height

    # Dưới ảnh: toàn bộ chữ D nằm dưới mép dưới bbox marker
    d_baseline = bottom - edge_clear - cap_h

    # Trên ảnh: B xoay 180° — baseline đủ cao để không tràn vào bbox
    b_baseline = top_edge + edge_clear + cap_h

    # Trái/phải: đẩy xa mép trái/phải (chữ xoay 90° cần bán kính ~cap)
    h_pad = edge_clear + cap_h * 0.72

    # Trên QR: B — xoay 180° (chân ra phía mép trên tờ)
    pdf.saveState()
    pdf.translate(cx, b_baseline)
    pdf.rotate(180)
    pdf.setFont(font, size)
    pdf.drawCentredString(0, 0, "B")
    pdf.restoreState()

    # Dưới QR: D — chân xuống mép ngoài
    pdf.saveState()
    pdf.translate(cx, d_baseline)
    pdf.setFont(font, size)
    pdf.drawCentredString(0, 0, "D")
    pdf.restoreState()

    # Phải QR: A — đổi chiều xoay so với bản cũ (+90° thay vì -90°)
    pdf.saveState()
    pdf.translate(left + width + h_pad, cy)
    pdf.rotate(90)
    pdf.setFont(font, size)
    pdf.drawCentredString(0, 0, "A")
    pdf.restoreState()

    # Trái QR: C — đổi chiều xoay so với bản cũ (-90° thay vì +90°)
    pdf.saveState()
    pdf.translate(left - h_pad, cy)
    pdf.rotate(-90)
    pdf.setFont(font, size)
    pdf.drawCentredString(0, 0, "C")
    pdf.restoreState()


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
    pupil_id_to_name: dict[int, str] | None,
    *,
    show_orientation_letters: bool = True,
    orientation_letter_size: float = 32,
    orientation_extra_edge_pt: float = 0.0,
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
    label_reserve = 52 if pupil_id_to_name is not None else 28

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
            max_h = cell_h - label_reserve
            scale = min(max_w / img_w, max_h / img_h)
            scale *= zoom
            draw_w = img_w * scale
            draw_h = img_h * scale
            if draw_w > max_w or draw_h > max_h:
                cap = min(max_w / draw_w, max_h / draw_h)
                draw_w *= cap
                draw_h *= cap
            draw_x = x + (cell_w - draw_w) / 2
            region_bottom = y + label_reserve
            region_height = cell_h - label_reserve
            draw_y = region_bottom + (region_height - draw_h) / 2

            pdf.drawImage(
                image,
                draw_x,
                draw_y,
                width=draw_w,
                height=draw_h,
                preserveAspectRatio=True,
                mask="auto",
            )
            if show_orientation_letters and orientation_letter_size > 0:
                draw_orientation_letters_around_qr(
                    pdf,
                    left=draw_x,
                    bottom=draw_y,
                    width=draw_w,
                    height=draw_h,
                    size=orientation_letter_size,
                    extra_edge_pt=orientation_extra_edge_pt,
                )
            if pupil_id_to_name is not None:
                label = pupil_id_to_name.get(marker_id) or f"id = {marker_id}"
            else:
                label = f"id = {marker_id}"
            draw_label_below_cell(
                pdf,
                cx=x + cell_w / 2,
                y_bottom=y + 6,
                text=label,
                max_width=cell_w - 12,
            )

        pdf.showPage()

    pdf.save()


def resolve_output_path(output_arg: str, *, data_subdir: str = "data") -> Path:
    """Đường dẫn tương đối được lưu trong thư mục con `data/` (cạnh main.py) theo mặc định."""
    script_dir = Path(__file__).resolve().parent
    output_path = Path(output_arg)
    if output_path.is_absolute():
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path
    base = script_dir / data_subdir
    out = (base / output_path).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    return out


def resolve_list_pupils_url(args: argparse.Namespace) -> str:
    if getattr(args, "list_pupils_url", None):
        return str(args.list_pupils_url)
    base = str(args.pupils_api_base).rstrip("/")
    return f"{base}/api/class-year/{args.class_year_id}/list-pupils?paginate=0"


def resolve_login_url(args: argparse.Namespace) -> str:
    if getattr(args, "login_url", None):
        return str(args.login_url).rstrip("/")
    base = str(args.pupils_api_base).rstrip("/")
    return f"{base}/api/auth-service/login"


def resolve_bearer_token(args: argparse.Namespace) -> str:
    token = (getattr(args, "bearer_token", None) or "").strip()
    token_file = getattr(args, "token_file", None)
    if not token and token_file:
        path = Path(token_file)
        token = path.read_text(encoding="utf-8").strip()
    if not token:
        token = os.environ.get("EDULIVE_BEARER_TOKEN", "").strip()
    return token


def resolve_login_credentials(args: argparse.Namespace) -> tuple[str, str]:
    username = (getattr(args, "username", None) or "").strip()
    password = (getattr(args, "password", None) or "").strip()
    if not username:
        username = os.environ.get("EDULIVE_USERNAME", "").strip()
    if not password:
        password = os.environ.get("EDULIVE_PASSWORD", "").strip()
    pw_file = getattr(args, "password_file", None)
    if not password and pw_file:
        password = Path(pw_file).read_text(encoding="utf-8").strip()
    return username, password


def resolve_pupil_bearer_token(args: argparse.Namespace) -> str:
    token = resolve_bearer_token(args)
    if token:
        return token
    username, password = resolve_login_credentials(args)
    if not username or not password:
        raise SystemExit(
            "Thiếu token và thông tin đăng nhập. Cách dùng:\n"
            "  - Token: EDULIVE_BEARER_TOKEN, hoặc --bearer-token, hoặc --token-file\n"
            "  - Hoặc đăng nhập: EDULIVE_USERNAME + EDULIVE_PASSWORD "
            "(hoặc --username / --password / --password-file)\n"
            "  - Hoặc --no-pupil-names để bỏ qua list-pupils."
        )
    login_url = resolve_login_url(args)
    return login_fetch_id_token(
        login_url=login_url,
        username=username,
        password=password,
        timeout=args.timeout,
        retries=args.retries,
    )


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
        "--output",
        default="aruco_4up.pdf",
        help="Tên file PDF. Đường dẫn tương đối lưu trong --data-dir (mặc định: thư mục data/).",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Thư mục con (trong folder chứa main.py) để ghi PDF; mặc định: data",
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
    parser.add_argument(
        "--pupils-api-base",
        default="https://school-beta-api.edulive.net",
        help="Base URL for school API (list-pupils is built from this + class-year id).",
    )
    parser.add_argument(
        "--class-year-id",
        type=int,
        default=66,
        help="Class year id in list-pupils path (default: 66).",
    )
    parser.add_argument(
        "--list-pupils-url",
        default=None,
        help="Full list-pupils URL (overrides --pupils-api-base and --class-year-id).",
    )
    parser.add_argument(
        "--bearer-token",
        default="",
        help="Bearer token for list-pupils. Prefer env EDULIVE_BEARER_TOKEN or --token-file.",
    )
    parser.add_argument(
        "--token-file",
        default=None,
        help="Path to a text file containing the Bearer token (one line).",
    )
    parser.add_argument(
        "--no-pupil-names",
        action="store_true",
        help="Do not call list-pupils; label markers with id = ... only.",
    )
    parser.add_argument(
        "--login-url",
        default=None,
        help="Login URL (default: {pupils-api-base}/api/auth-service/login).",
    )
    parser.add_argument(
        "--username",
        default="",
        help="Login username if no token (or use env EDULIVE_USERNAME).",
    )
    parser.add_argument(
        "--password",
        default="",
        help="Login password if no token (or use env EDULIVE_PASSWORD).",
    )
    parser.add_argument(
        "--password-file",
        default=None,
        help="File containing login password (if not using --password / env).",
    )
    parser.add_argument(
        "--no-orientation-letters",
        action="store_true",
        help="Do not draw A/B/C/D around each marker.",
    )
    parser.add_argument(
        "--orientation-letter-size",
        type=float,
        default=32,
        help="Font size for A/B/C/D orientation letters (default: 32).",
    )
    parser.add_argument(
        "--orientation-extra-edge",
        type=float,
        default=0.0,
        help="Thêm khoảng cách (pt) giữa mép ảnh marker và chữ A/B/C/D (mặc định: 0).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    _script_dir = Path(__file__).resolve().parent
    load_env_file(_script_dir / "data" / ".env")
    load_env_file(_script_dir / ".env", override=True)
    args = parse_args()
    output_path = resolve_output_path(args.output, data_subdir=args.data_dir)
    pupil_id_to_name: dict[int, str] | None = None
    if not args.no_pupil_names:
        token = resolve_pupil_bearer_token(args)
        list_url = resolve_list_pupils_url(args)
        pupil_id_to_name = fetch_pupil_id_to_name(
            list_pupils_url=list_url,
            bearer_token=token,
            timeout=args.timeout,
            retries=args.retries,
        )
    build_pdf(
        output_path=str(output_path),
        base_url=args.base_url,
        class_id=args.class_id,
        start_id=args.start_id,
        count=args.count,
        size=args.size,
        zoom=args.zoom,
        per_page=args.per_page,
        timeout=args.timeout,
        retries=args.retries,
        pupil_id_to_name=pupil_id_to_name,
        show_orientation_letters=not args.no_orientation_letters,
        orientation_letter_size=args.orientation_letter_size,
        orientation_extra_edge_pt=args.orientation_extra_edge,
    )
