# Khai báo sử dụng AI

**Công cụ:** Claude Code (Anthropic, model Claude Opus 5.5) chạy trong terminal trên máy cá nhân.

**Phạm vi hỗ trợ:**

- Đọc hướng dẫn lab (README, `docs/`, trang VLearn) và tóm tắt các bước, tiêu chí chấm.
- Chạy lệnh cài đặt, smoke test, pytest, `run_all.py` và thực thi 8 notebook bằng `nbconvert`; các số liệu trong notebook là output thật từ máy tôi.
- Viết 2 cell bằng chứng bổ sung (NB1: in nội dung `_delta_log/`; NB4: kiểm tra đầy đủ điều kiện Gold) — không sửa code gốc, không hạ ngưỡng hay bỏ assertion.
- Soạn nháp phần giải thích kết quả cuối mỗi notebook, `INFO.md`, reflection; script render và chụp ảnh màn hình bằng headless Chrome.
- Hỗ trợ điều tra hai điểm bất thường khi đối chiếu output:
  NB4 có 8 ngày thay vì 7 vì `CAST(ts AS DATE)` dùng múi giờ UTC+7 của máy;
  NB6 đếm "15 file trên đĩa" vì `count_files()` tính cả checkpoint Parquet trong `_delta_log/`.

- Bonus: soạn nháp `bonus/ARCHITECTURE.md` (Topic A) và viết PoC `bonus/poc/pii_tokenize_purge.py`; số liệu trong tài liệu lấy từ
  output PoC và notebook của tôi. Giá cloud là giá list tham khảo được ghi rõ là giả định.

**Tôi tự thực hiện:** kiểm tra lại output, đọc và chỉnh sửa phần giải thích, chọn anti-pattern và nội dung reflection, quyết định nộp bài.

Không có output bị chỉnh tay, không có số liệu tự tạo, không có API key hay dữ liệu cá nhân trong repo.
