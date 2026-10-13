# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Bùi Đức Thành |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602364 |
| GitHub | https://github.com/n4hhh/K4-L3L4-Track4-DataPipeline4ComputerVision |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-04622 |
## Kết quả cuối

- `dataset_version` của bạn: `a58541852303`
- Số khung hình train / val / test của bạn: 679 / 154 / 186
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1


Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?


Nguồn A có 3.021 dòng telemetry, còn nguồn B có 3.234 dòng. Hai nguồn có nhiều khác biệt về schema: (1) A sử dụng `ts_utc` theo ISO 8601 UTC, B sử dụng `timestamp_ms` dạng epoch milliseconds; (2) tốc độ của A là `speed_mps` (m/s), B là `spd_kmh` (km/h); (3) hướng di chuyển lần lượt là `heading_deg` và `hdg`; (4) tọa độ là `lat/lon` và `latitude/longitude`; (5) `gas/brake` của A biểu diễn tỷ lệ, còn `throttle_pct/brake_pct` của B sử dụng phần trăm.

Pipeline giữ hai nguồn trong hai bảng Bronze riêng và lưu giá trị thô dưới dạng chuỗi để bảo toàn dữ liệu gốc, kể cả giá trị thiếu hoặc chưa hợp lệ. Việc ép kiểu, đổi đơn vị, đồng bộ timestamp và xác định dòng lỗi được thực hiện ở Silver, giúp truy vết và kiểm tra lại dữ liệu nguồn.

Thí nghiệm ingest cho thấy lần nạp lại trả về `already present`. Bảng `bronze.raw_telemetry_a` chỉ có một commit Delta (`version 0`, `rows_added = 3021`), chứng minh dữ liệu không bị nhân đôi. Bảng video cũng chỉ ghi nhận hai file duy nhất.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.



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



Test `max_gap_ms` được cấu hình `warn` vì khoảng trống dữ liệu telemetry do mất tín hiệu cảm biến có thể xảy ra trong thực tế. Pipeline cần cảnh báo để điều tra, nhưng không nhất thiết phải dừng toàn bộ quá trình xử lý. Ngược lại, `in_range` kiểm tra tốc độ trong khoảng 0–70 m/s và sử dụng severity `error`, vì tốc độ nằm ngoài miền hợp lệ có thể làm sai lệch các phân tích và dataset phía sau.

Trong thí nghiệm `break-gate`, giới hạn làm sạch được nới từ 70 lên 1.000 m/s. Kết quả dbt test chuyển thành `PASS=5 WARN=1 ERROR=1 SKIP=0`. Test `in_range_silver_telemetry_speed_mps__70__0` phát hiện **6 dòng vi phạm**, khiến quality gate thất bại.

Sau khi khôi phục giới hạn 70 m/s, kết quả trở về `PASS=6 WARN=1 ERROR=0 SKIP=0`.

Thí nghiệm chứng minh quality gate là lớp kiểm tra độc lập với quy tắc làm sạch. Nếu bước làm sạch được cấu hình sai, test vẫn có thể phát hiện dữ liệu không hợp lệ. Test `error` ngăn dữ liệu xấu đi tiếp, còn `warn` cho phép pipeline tiếp tục nhưng vẫn ghi nhận vấn đề cần theo dõi.
## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?



Thí nghiệm idempotency được thực hiện trên partition `trip-00003`, có 60 frame. Trước khi chạy lại, bảng Silver có 1.246 dòng và fingerprint `6c7480dfde7e`. Sau hai lần chạy lại cùng trip, bảng vẫn có 1.246 dòng và fingerprint không thay đổi, chứng minh thao tác có tính idempotent cả về số lượng lẫn nội dung dữ liệu.

Ngược lại, khi mô phỏng cách ghi `append` sai, một trip có 60 frame trở thành 120 dòng sau hai lần ghi, tức dữ liệu bị nhân đôi (2x).

Delta `overwrite` theo predicate `trip_id` thay thế dữ liệu thuộc partition cần xử lý và commit thay đổi theo cơ chế transaction. Cách này an toàn hơn việc xóa rồi ghi lại bằng hai thao tác riêng biệt: nếu quá trình bị lỗi giữa hai thao tác, dữ liệu có thể rơi vào trạng thái thiếu hoặc không nhất quán. Nhờ atomic commit và cơ chế versioning của Delta, pipeline hỗ trợ chạy lại từng trip mà không làm nhân đôi dữ liệu.
### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |
Khung hình đen (`is_black`) là frame có độ sáng cực thấp, thường không chứa đủ thông tin thị giác hữu ích. Khung hình tối (`is_dark`) vẫn có thể chứa đặc trưng quan trọng của đường, xe và môi trường trong điều kiện ánh sáng yếu. Pipeline loại khung đen nhưng chỉ gắn cờ khung tối, cho phép giữ lại nếu frame không vi phạm các điều kiện chất lượng khác. Tôi đồng ý vì điều này giúp dataset phản ánh nhiều điều kiện ánh sáng thực tế.

Ở Silver, drive A có 6 frame đen, 9 frame mờ, 4 frame tối và 149 frame gần trùng. Drive B có 4 frame đen, 0 frame mờ, 61 frame tối và 53 frame gần trùng.

**Kết quả thí nghiệm dHash:**

| Ngưỡng dHash | Drive A | Drive B |
|---|---:|---:|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Tôi chọn ngưỡng **2**, trùng với cấu hình pipeline, vì đây là mức cân bằng giữa phát hiện frame gần giống và bảo toàn sự đa dạng thị giác. Ngưỡng 0 có thể bỏ sót frame gần trùng, trong khi ngưỡng 6 quá nhạy, có nguy cơ loại cả những frame chứa thay đổi cảnh vật có ý nghĩa.

Ngưỡng dHash cũng liên quan đến tốc độ xe: xe đứng yên hoặc di chuyển chậm thường tạo ra nhiều frame liên tiếp giống nhau, còn khi xe chạy nhanh, nội dung giữa các frame thay đổi lớn hơn. Vì vậy, một ngưỡng cố định không nhất thiết tối ưu cho mọi tốc độ và điều kiện môi trường.
## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?



Theo kết quả `leakage.json`, dataset Gold có 1.019 frame hợp lệ thuộc 26 trip. Khi chia ngẫu nhiên theo từng frame với tỷ lệ 70/15/15, cả **26/26 trip** đều xuất hiện trong nhiều split, đồng thời cả 26 trip đều có frame ở cả train và test. Ngược lại, khi chia theo trip như pipeline hiện tại, số trip bị chia cắt là **0/26**.

Việc chia theo frame gây data leakage vì các frame liên tiếp trong cùng chuyến đi thường có nội dung thị giác, điều kiện ánh sáng và bối cảnh rất giống nhau. Mô hình có thể ghi nhớ các đặc trưng đã xuất hiện trong train thay vì học cách tổng quát hóa sang cảnh mới. Điều này khiến metric trên test cao một cách giả tạo và không phản ánh đúng khả năng hoạt động trên các chuyến đi chưa từng thấy.

Chia theo trip bảo đảm toàn bộ frame của một trip chỉ thuộc một split, tạo sự tách biệt hợp lý hơn giữa dữ liệu huấn luyện và kiểm thử. Tuy nhiên, các trip khác nhau vẫn có thể chứa cảnh tương tự, nên cách chia này chưa loại bỏ mọi dạng leakage có thể xảy ra.
### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?



Dataset Gold của tôi có `dataset_version = a58541852303`, gồm **679 frame train, 154 frame validation và 186 frame test**, tổng cộng 1.019 frame thuộc 26 trip (18 train, 4 validation, 4 test).

`dataset_version` được tính bằng SHA256 từ danh sách `frame_id` và split tương ứng, lấy 12 ký tự đầu của hash. Vì vậy, phiên bản phản ánh nội dung và cách phân chia dataset thay vì thời điểm chạy hay một số đếm tăng dần.

Sau khi thực hiện `export-dataset` lần thứ hai, phiên bản vẫn là `a58541852303`, số frame và số trip trong từng split cũng không đổi, chứng minh tính ổn định khi cùng đầu vào được xử lý lại.

Pipeline sử dụng MSSV `2A202602364` làm salt trong phép MD5 sắp xếp trip trước khi phân chia train/val/test. Vì vậy, những MSSV khác nhau có thể tạo ra cách phân chia và `dataset_version` khác nhau dù sử dụng cùng nguồn dữ liệu.
### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?



Dataset Gold của tôi có phiên bản `a58541852303`, gồm 1.019 frame hợp lệ, được chia thành train (679), validation (154) và test (186). Theo `manifest.json`, dữ liệu được tạo từ các bảng và phiên bản sau:

- **Bronze:** `raw_telemetry_a` v0, `raw_telemetry_b` v0, `trips_raw` v0 và `video_files` v1.
- **Silver:** `frames` v27, `telemetry` v0, `frames_flagged` v0 và `frame_telemetry` v0.
- **Video gốc:** SHA256 drive A là `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; drive B là `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`.

Pipeline loại các frame có `is_black`, `is_blurry`, `is_near_dup` hoặc không đạt `sync_ok`, sau đó chia dataset theo trip, phân tầng theo drive và sắp xếp bằng MD5 có sử dụng MSSV.

Để truy vết hoặc tái tạo dataset sau ba tháng, cần lưu phiên bản dữ liệu nguồn, SHA256 video, cấu hình xử lý, mã pipeline, MSSV và quy tắc chia split. `manifest.json` cung cấp lineage và quy tắc lọc/split, giúp đối chiếu dữ liệu đầu ra với các nguồn đã sử dụng.
## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Trong thử nghiệm Airflow, tôi cấu hình `LAB_FAIL_ONCE=trip-04622` rồi chạy lại DAG từ lake trống. Trong 26 mapped task Silver Frames, `silver_frames[12]` thất bại với thông báo `RuntimeError: simulated crash while extracting trip-04622`; 25 task còn lại thành công. Bảy task phụ thuộc phía sau chuyển sang `upstream_failed`.

Tôi chỉ clear đúng mapped task có `map_index=12`, không clear cả nhóm 26 trip. Khi chạy lại, task này thành công và các task downstream tiếp tục cho đến khi toàn bộ DAG xanh.

Sau recovery, lệnh verify đạt **27/27 checks passed**, Silver có 1.246 frame, Gold có 1.019 frame, `dataset_version` vẫn là `a58541852303` như trước khi crash. Đây là bằng chứng dữ liệu cuối không bị nhân đôi và dataset ổn định sau retry. Screenshot: `fail_task_log.png`, `airflow_graph.png`, `airflow_retry_graph.png`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Nếu nâng từ 2 drive lên **500 xe mỗi ngày**, tôi dự đoán ba điểm nghẽn chính:

1. **Trích video FFmpeg và khả năng điều phối Airflow:** `silver_frames` đang giới hạn một mapped task chạy đồng thời trên mỗi DAG run. Số trip tăng khiến hàng đợi xử lý dài. Tôi đề xuất tách worker và tăng concurrency theo tài nguyên thực tế, có thể dùng CeleryExecutor hoặc KubernetesExecutor.
2. **MinIO một node:** lượng video, frame và các request tăng làm áp lực dung lượng, băng thông và I/O lớn hơn. Có thể nâng sang MinIO distributed hoặc object storage như S3, kèm phân vùng lưu trữ và chính sách lifecycle.
3. **DuckDB/dbt tập trung và tính toán lại toàn bảng:** truy vấn ETL trên lượng dữ liệu lớn dễ kéo dài. Tôi đề xuất mô hình incremental theo trip/ngày, xử lý theo partition và cân nhắc Spark/compute phân tán khi benchmark chứng minh cần thiết.

Đây là các điểm nghẽn dự báo theo thiết kế hiện tại, chưa phải kết quả benchmark thực tế tại 500 xe/ngày.

## Câu hỏi riêng của bạn trên Metabase

**Câu hỏi riêng:** Những trip nào có tỷ lệ frame bị loại cao nhất?

Tôi tạo câu SQL trong Metabase dựa trên bảng `marts.mart_trip_summary`:

    SELECT drive_id, trip_id, frames, frames_kept,
           frames - frames_kept AS frames_dropped,
           ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS drop_rate_pct,
           lighting, conditions, avg_speed_kmh
    FROM marts.mart_trip_summary
    ORDER BY drop_rate_pct DESC, trip_id
    LIMIT 5;

Kết quả: `trip-04622` loại 34/60 frame (56,67%), `trip-03904` 11/22 (50,00%), `trip-02086` 28/60 (46,67%), `trip-08888` 24/60 (40,00%) và `trip-07774` 13/34 (38,24%). Cả năm trip thuộc drive A, lighting Day và conditions Clouds. Tốc độ trung bình dao động 6,5–67,1 km/h, do đó không đủ cơ sở kết luận tốc độ cao là nguyên nhân chính. Cần phân tích thêm từng lý do loại: black, blurry, near-duplicate và sync loss.

## Nhìn lại

Điều bất ngờ nhất với tôi là cả 26 trip đều có frame xuất hiện đồng thời ở train và test khi chia ngẫu nhiên theo frame; cách chia theo trip loại được rò rỉ dạng này. Thí nghiệm task crash cũng thể hiện vai trò của idempotency khi chỉ chạy lại một partition mà kết quả Gold vẫn giữ nguyên.

Khó khăn thực tế là shell script bị đổi LF thành CRLF khi làm việc trên Windows, và kết quả verify được PowerShell ghi ra UTF-16 thay vì UTF-8. Một hạn chế cần tìm hiểu thêm là dHash có thể bỏ sót ảnh gần trùng về ngữ nghĩa; pipeline cũng chưa chứng minh được các trip khác nhau không ghi lại những cảnh rất tương tự.
