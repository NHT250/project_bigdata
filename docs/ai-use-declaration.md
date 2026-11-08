# Tuyên bố sử dụng AI

Theo mục "Source and AI-use integrity" của kế hoạch môn học Big Data Analytics (72ITDS40303).

## AI đã được dùng vào việc gì

Nhóm dùng **Claude** (Anthropic) để hỗ trợ:

- thiết kế kiến trúc hệ thống và viết đặc tả (`docs/superpowers/specs/`);
- lập kế hoạch triển khai theo từng bước có test (`docs/superpowers/plans/`);
- viết mã nguồn và test trong `src/`, `tests/`, `evidence/`;
- soạn nội dung tài liệu (README, docstring, chú thích).

## Những gì nhóm tự làm và tự chịu trách nhiệm

- Mọi **số liệu, biểu đồ và bằng chứng thực nghiệm** trong báo cáo do nhóm tự tạo bằng cách chạy hệ thống trên dữ liệu thật, và tự kiểm tra.
- Mọi **trích dẫn** trong báo cáo do nhóm tự đọc và kiểm tra nguồn gốc.
- Nhóm đọc, chạy và hiểu toàn bộ mã nguồn. Cả ba thành viên phải giải thích được mọi phần khi vấn đáp.

## Những gì không làm

- **Không** trích dẫn phản hồi của AI làm bằng chứng kỹ thuật.
- **Không** dùng số liệu ở chế độ demo của các script `evidence/` (dữ liệu tổng hợp) làm kết quả trong báo cáo.

## Thành phần AI bên trong hệ thống

Tách biệt với việc dùng AI để viết code, hệ thống có ba tính năng mang tính "AI" và nhóm khai báo đúng bản chất từng cái (spec §6.4):

| Tính năng | Bản chất | Nhóm tự huấn luyện? |
|---|---|---|
| Phát hiện edit rủi ro | GBTClassifier (Spark MLlib), học có giám sát trên nhãn tự sinh từ revert | **Có** |
| Phát hiện sự kiện nóng | Thống kê cửa sổ trượt, z-score so với đường nền EWMA | Không, không phải AI |
| Tóm tắt thay đổi | Khuôn mẫu; tuỳ chọn gọi LLM có sẵn | Không |
| Chat hỏi đáp sinh SQL | Mô hình ngôn ngữ có sẵn (Claude) gọi qua API, kèm rào chắn SQL | **Không**, nhóm chỉ sử dụng |

Slide cuối của bài trình bày nhắc lại tuyên bố này.
