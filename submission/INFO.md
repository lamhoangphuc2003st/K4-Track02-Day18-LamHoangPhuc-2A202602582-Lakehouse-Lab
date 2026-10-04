# Thông tin bài nộp

| Mục | Giá trị |
|---|---|
| Họ tên | Lâm Hoàng Phúc (`LamHoangPhuc`) |
| MSSV | 2A202602582 |
| Mã bài | K4-Track02-Day18 — Data Lakehouse Architecture |
| Repo | `K4-Track02-Day18-LamHoangPhuc-2A202602582-Lakehouse-Lab` |
| Đường chạy | **Lightweight** cho cả 8 notebook (NB1–NB4 **không** dùng Spark) |
| Hệ điều hành | Windows 10 Home 22H2 (10.0.19045), PowerShell / Git Bash |
| Python | 3.11.7 (venv `.venv`) |

## Phiên bản thư viện chính

`deltalake 1.6.6` · `pyiceberg 0.12.0` (`pyiceberg-core 0.10.1`) · `duckdb 1.5.6` · `polars 1.44.2` ·
`pyarrow 25.0.1` · `numpy 2.4.6` · `jupyterlab 4.6.4` · `nbconvert 7.17.1`

## Kết quả kiểm tra (log đầy đủ trong `submission/logs/`)

| Lệnh (PowerShell equivalent của `make`) | Kết quả |
|---|---|
| `scripts/verify_lite.py` (smoke) | 9/9 PASS — [smoke.txt](logs/smoke.txt) |
| `python -m pytest` | 24/24 PASS — [pytest.txt](logs/pytest.txt) |
| `scripts/run_all.py` | 8/8 PASS — [run_all.txt](logs/run_all.txt) |

## Cách tạo notebook nộp

1. `jupytext --to notebook notebooks/0*.py` để sinh `.ipynb`.
2. Thêm 2 cell bằng chứng (không sửa code gốc của lab):
   NB1 — in danh sách `_delta_log/` và nội dung 2 commit JSON;
   NB4 — in toàn bộ Gold và tự kiểm tra ≥ 7 ngày × 3 model, p50 ≤ p95, cost > 0, error_rate ∈ [0, 1].
3. Thực thi cả 8 notebook theo thứ tự với `jupyter nbconvert --execute`, giữ output.
4. Thêm cell Markdown **"📝 Giải thích kết quả (học viên)"** cuối mỗi notebook, dùng số đo của lần chạy đó.

Số liệu thời gian (NB2 speedup, NB7 thời gian scan) thay đổi theo từng lần chạy; số trong phần giải thích khớp với output đã lưu trong notebook.

## Danh mục bài nộp

- `notebooks/` — 8 notebook đã thực thi
- `screenshots/` — ảnh kết quả chính của mỗi notebook (`nb01_delta_log.png`, `nb01_schema.png`, `nb02_optimize.png`, `nb03_history.png`,
  `nb04_gold.png`, `nb05_iceberg.png`, `nb06_maintenance.png`, `nb07_vectors.png`, `nb08_provenance.png`)
- [`REFLECTION.md`](REFLECTION.md), [`AI_USAGE.md`](AI_USAGE.md)
- **Bonus:** [`bonus/ARCHITECTURE.md`](bonus/ARCHITECTURE.md) — Topic A, LLM observability 1B req/ngày;
  PoC [`bonus/poc/pii_tokenize_purge.ipynb`](bonus/poc/pii_tokenize_purge.ipynb) (tokenize PII tại Bronze + purge khỏi time travel),
  chạy lại bằng `.\.venv\Scripts\python.exe submission/bonus/poc/pii_tokenize_purge.py`
