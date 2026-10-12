# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `<điền>` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `<điền>` |
| GitHub | `<điền: tên tài khoản>` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
(trả lời ở đây)

Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?


Nguồn A có 3.021 dòng telemetry, còn nguồn B có 3.234 dòng. Hai nguồn có nhiều khác biệt về schema: (1) A sử dụng `ts_utc` theo ISO 8601 UTC, B sử dụng `timestamp_ms` dạng epoch milliseconds; (2) tốc độ của A là `speed_mps` (m/s), B là `spd_kmh` (km/h); (3) hướng di chuyển lần lượt là `heading_deg` và `hdg`; (4) tọa độ là `lat/lon` và `latitude/longitude`; (5) `gas/brake` của A biểu diễn tỷ lệ, còn `throttle_pct/brake_pct` của B sử dụng phần trăm.

Pipeline giữ hai nguồn trong hai bảng Bronze riêng và lưu giá trị thô dưới dạng chuỗi để bảo toàn dữ liệu gốc, kể cả giá trị thiếu hoặc chưa hợp lệ. Việc ép kiểu, đổi đơn vị, đồng bộ timestamp và xác định dòng lỗi được thực hiện ở Silver, giúp truy vết và kiểm tra lại dữ liệu nguồn.

Thí nghiệm ingest cho thấy lần nạp lại trả về `already present`. Bảng `bronze.raw_telemetry_a` chỉ có một commit Delta (`version 0`, `rows_added = 3021`), chứng minh dữ liệu không bị nhân đôi. Bảng video cũng chỉ ghi nhận hai file duy nhất.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

(trả lời ở đây)

Pipeline chuẩn hóa telemetry A và B về cùng schema tại Silver. Nguồn B được đổi tên cột, chuyển tốc độ từ km/h sang m/s bằng cách chia 3,6, chuyển timestamp epoch milliseconds sang UTC và trừ clock offset 1.500 ms. Các giá trị `throttle_pct` và `brake_pct` cũng được chia 100 để đưa về tỷ lệ 0–1.

Sau chuẩn hóa, thời điểm đầu tiên của drive B là `2025-10-08 14:30:00.009 UTC`, chỉ lệch 9 ms so với thời điểm bắt đầu video (`14:30:00`). Điều này chứng minh độ lệch đồng hồ đã được xử lý.

**Kết quả quarantine thực nghiệm:**

| Lý do | drive_a | drive_b |
|---|---:|---:|
| duplicate | 20 | 20 |
| speed_missing | 30 | 30 |
| speed_out_of_range | 3 | 3 |
| **Tổng** | **53** | **53** |

Sau xử lý, drive A còn 2.968 dòng sạch và drive B còn 3.181 dòng sạch. Tổng cộng 6.149 dòng hợp lệ và 106 dòng quarantine, khớp 6.255 dòng Bronze ban đầu.

Dữ liệu lỗi được đưa vào `silver_telemetry_rejects` cùng `reject_reason` thay vì xóa, giúp kiểm toán, truy vết nguồn lỗi và cải thiện chất lượng dữ liệu mà không làm mất bằng chứng gốc.
### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

(trả lời ở đây)

Test `max_gap_ms` được cấu hình `warn` vì khoảng trống dữ liệu telemetry do mất tín hiệu cảm biến có thể xảy ra trong thực tế. Pipeline cần cảnh báo để điều tra, nhưng không nhất thiết phải dừng toàn bộ quá trình xử lý. Ngược lại, `in_range` kiểm tra tốc độ trong khoảng 0–70 m/s và sử dụng severity `error`, vì tốc độ nằm ngoài miền hợp lệ có thể làm sai lệch các phân tích và dataset phía sau.

Trong thí nghiệm `break-gate`, giới hạn làm sạch được nới từ 70 lên 1.000 m/s. Kết quả dbt test chuyển thành `PASS=5 WARN=1 ERROR=1 SKIP=0`. Test `in_range_silver_telemetry_speed_mps__70__0` phát hiện **6 dòng vi phạm**, khiến quality gate thất bại.

Sau khi khôi phục giới hạn 70 m/s, kết quả trở về `PASS=6 WARN=1 ERROR=0 SKIP=0`.

Thí nghiệm chứng minh quality gate là lớp kiểm tra độc lập với quy tắc làm sạch. Nếu bước làm sạch được cấu hình sai, test vẫn có thể phát hiện dữ liệu không hợp lệ. Test `error` ngăn dữ liệu xấu đi tiếp, còn `warn` cho phép pipeline tiếp tục nhưng vẫn ghi nhận vấn đề cần theo dõi.
## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

(trả lời ở đây)

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | | |
| 2 | | |
| 6 | | |

(trả lời ở đây)

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

(trả lời ở đây)

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

(trả lời ở đây)

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

(trả lời ở đây)

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
