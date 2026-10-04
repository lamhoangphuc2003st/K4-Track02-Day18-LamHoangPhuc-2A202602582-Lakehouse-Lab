# Architecture Brief — LLM Observability ở quy mô 1B requests/ngày

**Topic A** · Lâm Hoàng Phúc · 2A202602582 · K4-Track02-Day18 (bonus, cá nhân)
PoC: [`poc/pii_tokenize_purge.ipynb`](poc/pii_tokenize_purge.ipynb)

---

## 1. Problem statement

Một team cung cấp foundation-model API cần log **mọi** request/response: **1B request/ngày × ~5 KB = 5 TB/ngày raw**,
trung bình 11.6K req/s, giả định peak 3× ≈ 35K req/s (≈ 175 MB/s). Yêu cầu:

1. Dashboard cost & latency **theo tenant**, refresh mỗi **5 phút**.
2. Prompt/response đầy đủ giữ **7 ngày** cho incident review; sau đó chỉ giữ **aggregates 1 năm**.
3. **PII phải được redact trước khi bất kỳ ai đọc** — kể cả data engineer.
4. Tổng chi phí **storage ≤ $5K/tháng**.

Vì sao khó: (a) volume không phải vấn đề chính — 5 TB/ngày nén tốt — mà là **lifecycle**: nếu xóa không thật sự xóa
(time travel, tombstone, orphan) thì 5 TB/ngày × 365 ≈ 1.8 PB ≈ $42K/tháng; (b) "redact trước khi ai đọc" xung đột với
khả năng replay khi bộ redact có bug, và với time travel vốn giữ lại bản cũ; (c) dashboard 5 phút theo tenant cần
p95 latency đúng khi roll-up — không thể lấy trung bình các p95.

**Giả định chính:** AWS us-east-1, giá list tham khảo (S3 Standard $0.023/GB-tháng, Standard-IA $0.0125 với tối thiểu 30 ngày,
PUT $0.005/1K, GET $0.0004/1K, EBS gp3 $0.08/GB-tháng, EC2 m7g.xlarge ≈ $0.163/h, m7g.2xlarge ≈ $0.326/h) — cần kiểm tra lại
trước khi dùng thật. ~20K tenant, ~3 model/tenant. Mỗi request: ~0.4 KB metadata + ~4.6 KB prompt/response.

---

## 2. Architecture

```
                         ┌──────────────── INGEST PATH ─────────────────────────────────────────────┐
 API gateway ──► Kafka `llm.raw` (24h, RF3, ACL: chỉ redactor đọc, mã hóa)                          │
                         │                                                                          │
                         ▼                                                                          │
              Redactor/tokenizer (stream, 6× m7g.xlarge)  [C1 PII tokenization tại Bronze landing]  │
              • HMAC-SHA256 token tất định cho email/phone/ID  • redaction_version trên mỗi dòng     │
              • canary PII tự bơm vào 1/1000 request          • token→value vault (break-glass)     │
                         │ exactly-once: Delta txn (appId, batchId)  [C2 ACID]                      │
          ┌──────────────┴───────────────┐                                                          │
          ▼                              ▼                                                          │
 BRONZE  bronze.llm_payload           bronze.llm_events            [C3 Medallion]                    │
 (đã redact, zstd, 7d+48h)            (metadata thô, 7d+48h)                                         │
  part: date  cluster: tenant_id,ts    part: date                                                    │
          │                              │ parse + dedup request_id (watermark 15')                  │
          │                              ▼                                                          │
 SILVER   │                        silver.llm_calls (typed, schema enforced, 7d+48h)  [C4 schema]    │
          │                              │ Change Data Feed → streaming MERGE mỗi 1'                 │
          │                              ▼                                                          │
 GOLD     │                        gold.tenant_5min (35d) ─► gold.tenant_hourly (1 năm) ─► daily     │
          │                         cột: cost, tokens, error_rate, latency **KLL sketch**            │
          └──────────────────────────────┴──────────────────────────────────────────────────────────┘
 QUERY PATH:  Dashboard (Trino) ─► gold.*  (≤ 3' lag)
              Incident review: silver.llm_calls (tìm tenant/ts) ─► llm_payload (pruned by date+tenant)
              → payload chỉ qua role `incident-reviewer`, mọi lần đọc ghi audit  [C5 catalog = control plane]
 MAINTENANCE  [C6 FinOps/lifecycle]: hourly OPTIMIZE+Z-ORDER → 256 MB · DELETE date < today-7
              · VACUUM 48h · checkpoint 100 commits · weekly orphan sweep (disk − log, age ≥ 24h)
              · bytes_on_disk / bytes_in_log metric + budget alarm
```

Concept Day18 được áp dụng (không chỉ gọi tên): **C1** tokenization trước lần ghi lakehouse đầu tiên; **C2** ACID +
idempotent streaming commit; **C3** medallion tách thô/sạch/phục vụ; **C4** schema enforcement ở Silver;
**C5** catalog làm control plane cho quyền đọc payload; **C6** lifecycle (DELETE ≠ xóa vật lý, VACUUM, orphan) và
compaction/Z-order cho hot path "filter by tenant"; **time travel** dùng cho rollback Gold (F3) và là rủi ro cần purge (F1).

---

## 3. Quyết định chính và alternatives đã loại

### D1 — Redact PII ở đâu: **stream redactor trước Bronze**
- **Chọn:** service đọc Kafka, token hóa và ghi Bronze. Raw chỉ tồn tại trong Kafka 24h, mã hóa, ACL chỉ cho redactor —
  đủ để **replay** khi redactor có bug (F1), không ai khác đọc được.
- **Loại redact ở API gateway:** thêm latency vào serving path của mọi request (35K req/s) và gắn bộ redact vào release
  cycle của team API; khi regex có bug thì **không còn bản gốc** để redact lại → mất dữ liệu 7 ngày.
- **Loại redact ở Silver:** vi phạm yêu cầu (3) — Bronze chứa PII thô và ai có quyền Bronze đều đọc được; time travel của
  Bronze giữ PII thêm ít nhất bằng retention.
- **Trade-off chấp nhận:** regex/dictionary bắt được email, phone, số định danh nhưng **không bắt tên người trong văn bản tự do**.
  NER chạy cho 100% traffic tốn ~10× CPU; thay vào đó chạy NER trên mẫu 1% làm detector (F1) và khóa quyền đọc payload (D7).

### D2 — Tách payload và metadata: **hai bảng Bronze**
- **Chọn:** `llm_payload` (4.6 KB/req, chỉ incident review đọc) và `llm_events` (0.4 KB/req, nuôi Silver/Gold).
- **Loại một bảng rộng:** NB7 cho thấy projection pushdown làm cột blob gần như miễn phí với query phân tích, nên lý do không
  phải tốc độ — mà là **quyền** (grant theo bảng đơn giản hơn column mask) và **vòng đời**: muốn bỏ payload sau 7 ngày nhưng
  giữ metadata thì phải rewrite cả file.
- **Loại một object S3 cho mỗi request (pointer table):** 1B PUT/ngày × $0.005/1K = **$5,000/ngày** chỉ riêng request cost,
  và LIST/expire 7B object. NB7 cho thấy pointer chỉ đáng khi cần random-read từng blob lớn (frame ảnh), không phải text 5 KB.

### D3 — Table format: **Delta Lake**
- **Chọn Delta:** streaming writer idempotent bằng `txnAppId/txnVersion`; **Change Data Feed** dẫn Silver → Gold incremental và
  phát sự kiện delete; quy trình VACUUM/checkpoint đã đo ở NB6.
- **Loại Iceberg:** hidden partitioning `day(ts)` và partition evolution (NB5) rất hấp dẫn, nhưng 4 bảng × commit mỗi phút =
  5,760 commit/ngày, mỗi commit thêm manifest list + manifest; NB5 đo metadata = 290% data khi commit nhỏ và NB6 cho thấy
  expire snapshot (đường PyIceberg) không tự xóa file → thêm một job sweep phải vận hành. Changelog cho Gold incremental cũng
  phụ thuộc engine hơn CDF. Nếu sau này cần đa engine, cân nhắc Delta UniForm thay vì migrate.
- **Loại Hudi:** Merge-on-Read tối ưu cho upsert dày; workload này 99% append, upsert chỉ ở Gold (nhỏ). Hudi thêm timeline
  service/compaction riêng mà không đem lại lợi ích tương xứng; team đã quen Delta.

### D4 — Layout: **partition theo `date`, Z-order `(tenant_id, ts)`, file 256 MB**
- **Chọn:** payload 0.92 TB/ngày nén → ~3,600 file 256 MB/ngày. Compaction mỗi giờ gộp file streaming (~20 MB) và Z-order
  theo tenant, để incident review "tenant X, 14:00–14:30" chỉ chạm vài file (NB2: Z-order cho pruning 55×).
- **Loại partition theo tenant:** 20K tenant × 7 ngày = 140K partition; tenant nhỏ có file vài KB → đúng small-file
  anti-pattern (NB6: 200 file 51 KB) và metadata phình (NB5).
- **Loại partition theo giờ, không clustering:** filter theo tenant vẫn phải quét cả giờ (~38 GB nén) vì mọi file chứa mọi tenant
  → min/max chồng lấn, không prune được (NB6: 11/11 file trước clustering).

### D5 — Dashboard 5 phút: **Gold incremental với sketch percentile**
- **Chọn:** streaming micro-batch 1 phút đọc CDF của Silver, MERGE vào `gold.tenant_5min` (khóa tenant, model, window);
  latency lưu dưới dạng **KLL/t-digest sketch** để roll-up 5 phút → giờ → ngày vẫn tính đúng p50/p95. Lag dự kiến ≤ 3 phút.
- **Loại dashboard đọc Silver trực tiếp:** mỗi lần refresh quét ~1B dòng/ngày; 200 dashboard × 288 refresh/ngày là compute vô hạn.
- **Loại materialized view recompute toàn bộ mỗi 5 phút:** tính lại cả ngày mỗi lần; chi phí tăng tuyến tính theo giờ trong ngày.
- **Loại lưu sẵn p95 dạng float (như NB4):** đúng ở grain đã tính nhưng **không roll-up được** — trung bình các p95 5 phút không phải
  p95 của giờ. Sketch tốn ~200 B/dòng nhưng Gold vẫn chỉ ~4 GB/ngày.

### D6 — Lifecycle & tiering: **DELETE theo partition + VACUUM 48h, ở S3 Standard**
- **Chọn:** job hằng ngày `DELETE WHERE date < today − 7` (chỉ ghi tombstone), `VACUUM` retention 48h (đủ cho query dài nhất
  và rollback một lần ghi lỗi), sweep orphan hằng tuần, theo dõi `bytes_on_disk / bytes_in_log`.
- **Loại S3 lifecycle rule xóa theo prefix:** xóa file mà log vẫn tham chiếu → bảng hỏng (reader gặp FileNotFound), và không
  thấy được orphan/tombstone theo nghĩa của table format.
- **Loại tier xuống Standard-IA / Glacier:** dữ liệu sống 9 ngày. Standard: 9/30 × $23 = **$6.9 mỗi TB ghi**. IA tính tối thiểu
  30 ngày: 1 × $12.5 = **$12.5/TB** (gần 2×) cộng phí retrieval khi incident review; Glacier IR tối thiểu 90 ngày:
  3 × $4 = **$12/TB** và phí lấy lại. Gold 1 năm chỉ ~0.3 TB ($7/tháng) — tiering không đáng công vận hành.

### D7 — Catalog & quyền: **catalog có grant + audit làm control plane**
- **Chọn:** đăng ký mọi bảng qua catalog (vd. Unity Catalog); `llm_payload` chỉ grant cho role `incident-reviewer` theo ticket,
  thời hạn 24h, mọi lần đọc ghi vào bảng audit; IAM bucket payload chỉ cho query service — hai lớp.
- **Loại truy cập theo đường dẫn S3:** ai có `s3:GetObject` trên bucket đọc được toàn bộ prompt; không audit theo bảng, không lineage.
- **Loại Hive Metastore:** chỉ là danh bạ tên → vị trí; không có grant chi tiết hay audit, phải tự dựng lớp kiểm soát bên ngoài.

---

## 4. Failure modes (3 giờ sáng)

**F1 — Redactor bỏ sót một định dạng PII** *(time travel + VACUUM; đã PoC)*
Deploy mới đổi regex, số điện thoại dạng `+84…` lọt vào Bronze.
- **Detect:** canary PII do hệ thống tự bơm (1/1000 request) + scanner mỗi 5 phút trên mẫu Bronze; alert khi canary xuất hiện
  dạng thô. PoC: batch v1 có **673/5,000** dòng lộ PII, scanner bắt được.
- **Rollback:** (1) revoke grant `llm_payload`; (2) rollback redactor; (3) `DELETE WHERE redaction_version = 'v_bad'`, replay cửa sổ
  đó từ Kafka (24h) qua redactor đã sửa; (4) **bắt buộc** `VACUUM` retention 0 sau khi dừng reader dài — PoC chứng minh sau DELETE,
  time travel v0 vẫn trả **673** dòng PII và 1 file Parquet trên đĩa vẫn chứa canary; sau VACUUM còn 0 file và v0 báo FileNotFound.
  Nếu phát hiện muộn hơn 24h (quá retention Kafka) thì chỉ còn cách xóa và mất payload cửa sổ đó — đánh đổi chấp nhận.

**F2 — Lifecycle âm thầm ngừng, chi phí trôi** *(VACUUM không thấy orphan — NB6)*
Job VACUUM fail do quyền IAM đổi, hoặc writer streaming crash liên tục để lại file chưa commit.
- **Detect:** metric hằng ngày `bytes_on_disk(prefix) / bytes_in_log` (phép hiệu tập hợp của NB6) — alert khi > 1.2; alert khi
  Bronze payload > 12 TB; AWS budget alarm ở 60% của $5K. Mỗi ngày job im lặng thêm ~0.92 TB (~$21/tháng tích lũy); sau 6 tháng
  không ai phát hiện là ~166 TB ≈ $3.8K/tháng — **tự phá budget**.
- **Rollback:** chạy lại VACUUM; orphan sweep với age guard ≥ 24h (không xóa file của writer đang chạy); kiểm tra `count()` và
  một query time travel trong cửa sổ 48h trước/sau.

**F3 — Upstream đổi schema, dashboard cost rơi về 0** *(schema enforcement + RESTORE)*
Team API đổi `usage.input` → `usage.input_tokens`; parser Silver trả NULL, Gold báo cost giảm 90% — finance hoảng.
- **Detect:** data contract ở Silver: tỉ lệ NULL của `prompt_tokens` mỗi 5 phút > 1% → page on-call; kiểu dữ liệu sai bị
  schema enforcement chặn ngay (NB1). Thêm cột mới chỉ qua `schema_mode=merge` có review.
- **Rollback:** `RESTORE gold.tenant_5min` về version trước sự cố (NB3: RESTORE là commit mới, giữ lịch sử để điều tra); sửa parser
  map cả hai tên field; replay Bronze events (còn 7 ngày) → Silver → MERGE lại các window bị ảnh hưởng.

**F4 — Streaming writer restart, ghi trùng hoặc bùng nổ file nhỏ**
- **Detect:** số dòng Silver/Bronze theo `request_id` trùng > 0.1%; kích thước file trung bình partition hôm nay < 32 MB.
- **Rollback:** commit idempotent (`txnAppId`, `txnVersion`) để micro-batch chạy lại không ghi đôi; dedup Silver theo `request_id`
  trong watermark 15 phút; chạy OPTIMIZE thủ công cho partition bị phân mảnh.

---

## 5. Chi phí ước lượng ($/tháng)

### Storage (ràng buộc ≤ $5K)

| Thành phần | Phép tính | TB lưu | $/tháng |
|---|---|---:|---:|
| Kafka `llm.raw` 24h | 5 TB ÷ 2.5 (lz4) × RF3 = 6 TB EBS × $80/TB | 6.0 | 480 |
| Bronze payload | 4.6 TB/ngày ÷ 5 (zstd) = 0.92 TB/ngày × (7 + 2 ngày VACUUM) + 2 ngày file cũ sau compaction | 10.1 | 233 |
| Bronze events | 0.4 TB/ngày ÷ 8 = 50 GB/ngày × 9 ngày | 0.45 | 10 |
| Silver llm_calls | ~45 GB/ngày × 9 ngày | 0.4 | 9 |
| Gold 5 phút (35 ngày) + hourly/daily (1 năm) | 20K tenant × 3 model × 288 window × 250 B ≈ 4.3 GB/ngày × 35 + 0.36 GB/ngày × 365 | 0.28 | 7 |
| Delta log, checkpoint, token vault | ước lượng | < 0.1 | 5 |
| S3 request | PUT ~5M/tháng ($25) + GET dashboard ~86M/tháng ($35) | | 60 |
| **Tổng storage** | | **~17.3** | **≈ $805** (16% budget) |

**Độ nhạy:** nếu zstd chỉ đạt 3× thay vì 5× (prompt nhiều code/base64), payload thành 16.8 TB → tổng ≈ $960. Rủi ro thật là F2:
không có lifecycle, 5 TB/ngày raw × 365 × $23 ≈ **$42K/tháng**. Tỉ lệ nén là giả định — MVP phải đo trên payload thật.

### Compute (không có cap; ước lượng để kiểm tra tính khả thi)

| Thành phần | Phép tính | $/tháng |
|---|---|---:|
| Redactor | peak 160 MB/s payload ÷ ~15 MB/s/vCPU ≈ 11 vCPU, ×2 headroom → 6 × m7g.xlarge × $0.163 × 730 | 714 |
| Streaming Bronze → Silver → Gold | 3 × m7g.2xlarge × $0.326 × 730 | 714 |
| Maintenance (OPTIMIZE hourly, VACUUM, sweep) | 2 × m7g.2xlarge × 6 h/ngày × 30 | 117 |
| Kafka brokers | 3 broker × ~$0.20/h × 730 | 440 |
| Trino cho dashboard + incident review | 3 × m7g.2xlarge | 714 |
| **Tổng compute** | | **≈ $2,700** |

Tổng ≈ **$3.5K/tháng**, storage nằm rất xa cap — dư địa này để dành cho F2 và cho tăng trưởng traffic (~6× trước khi chạm cap).

---

## 6. MVP một tuần

**Slice:** 1 generator mô phỏng 3 tenant ở ~1% traffic (10M request/ngày, payload lấy mẫu từ prompt nội bộ đã ẩn danh) →
Kafka → redactor → Bronze payload + events → Silver → `gold.tenant_5min` → 1 dashboard; kèm job lifecycle chạy với "1 ngày = 1 giờ".

| Ngày | Việc |
|---|---|
| 1 | Redactor + tokenizer + canary, mở rộng từ PoC; bộ test 50 mẫu PII VN (phone `0…`/`+84…`, CCCD 12 số, email). |
| 2 | Streaming ghi Bronze idempotent; kill writer giữa batch, kiểm tra không trùng. |
| 3 | Silver dedup + data contract; Gold MERGE với sketch; so p95 roll-up giờ với p95 tính trực tiếp. |
| 4 | Lifecycle: DELETE + VACUUM + orphan sweep + metric `disk/log`; đo tỉ lệ nén zstd trên payload thật. |
| 5 | Diễn tập F1 và F3 end-to-end, đo thời gian khôi phục; viết runbook. |

**Tiêu chí nghiệm thu:**
1. 0 canary thô trong Bronze qua 10M request; 100% mẫu test PII được token hóa; token tất định (cùng user → cùng token).
2. Lag từ request → Gold ≤ 5 phút ở p95; query dashboard theo tenant trên Gold < 1 s.
3. p95 roll-up từ sketch lệch < 1% so với p95 exact trên cùng dữ liệu.
4. Sau lifecycle: `bytes_on_disk / bytes_in_log` ≤ 1.05, partition hết hạn có 0 byte trên đĩa, time travel vượt retention báo lỗi.
5. Tỉ lệ nén đo được ≥ 3× (ngưỡng để storage vẫn < $1K/tháng ở quy mô đầy đủ).

**Kiểm chứng cơ chế khó nhất** — "PII đã xóa thì phải thật sự biến mất": [PoC](poc/pii_tokenize_purge.ipynb) chạy offline bằng
`deltalake` của lab. Kết quả đo: redactor v1 lộ **673/5,000** dòng; canary scanner phát hiện; sau DELETE + replay v2, version hiện
tại có **0** dòng PII và **501** token email khác nhau (500 user + 1 canary — token tất định vẫn giữ được phép đếm/join);
nhưng time travel v0 vẫn trả **673** dòng PII cho tới khi VACUUM xóa **1** file, sau đó **0** file trên đĩa chứa canary và v0 báo
`FileNotFoundError`. Giới hạn của PoC: chạy cục bộ, regex đơn giản, không có Kafka hay catalog thật.

---

## Nguồn tham khảo

- Delta Lake docs — VACUUM, Change Data Feed, idempotent writes (`txnAppId`): https://docs.delta.io/
- Apache Iceberg — maintenance (expire snapshots, remove orphan files): https://iceberg.apache.org/docs/latest/maintenance/
- AWS S3 pricing (storage classes, minimum storage duration, request cost): https://aws.amazon.com/s3/pricing/
- Apache DataSketches — KLL quantile sketch: https://datasketches.apache.org/
- Số đo NB2, NB5, NB6, NB7 của chính tôi trong `submission/notebooks/`.
