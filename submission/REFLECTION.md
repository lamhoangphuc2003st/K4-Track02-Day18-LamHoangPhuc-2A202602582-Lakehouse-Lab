# Reflection — Small-file problem

**Anti-pattern chọn:** bùng nổ file nhỏ (small files) do ghi micro-batch.

Hệ thống tôi quan tâm là log gọi LLM và agent trace của các ứng dụng AI: mỗi request sinh một event nhỏ,
pipeline streaming commit vài giây một lần. Mỗi commit đều đúng, tích lũy lại thành lỗi. NB6 đo được trên máy tôi:
200 commit tạo 200 file trung bình 51.5 KB và 200 JSON trong log; point query phải mở 11/11 file vì min/max chồng lấn.
NB5 còn cho thấy cái giá thứ hai: với file nhỏ, metadata Iceberg lớn gấp 2.9 lần dữ liệu.

**Phòng tránh:**
1. Ở writer: tăng trigger interval/kích thước batch, nhắm 128–512 MB mỗi file thay vì sửa hậu quả.
2. Lịch maintenance định kỳ: compaction (200 → 11 file), clustering/Z-order theo cột hay lọc (skip 90%), checkpoint log.
3. Đi kèm expiry + orphan sweep với retention ≥ 7 ngày, vì vacuum của `deltalake` không thấy file chưa commit
   và expiry của PyIceberg không tự xóa manifest list.
4. Cảnh báo khi kích thước file trung bình mỗi partition quá nhỏ.

**AI:** dùng Claude Code để chạy lab, điều tra output và soạn nháp; chi tiết tại [AI_USAGE.md](AI_USAGE.md).
