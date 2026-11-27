# -*- coding: utf-8 -*-
"""Hàm hình học viết bằng numpy thuần (không cần pyproj, shapely).

Vì sao cần file này: pipeline Spark có thể chạy trong container Docker của học phần, nơi KHÔNG cài
pyproj/shapely. Các hàm dưới đây chỉ cần numpy nên chạy được ở mọi nơi:
  - utm49n(): đổi kinh độ/vĩ độ (WGS84) sang toạ độ phẳng UTM múi 49 Bắc (mét). Công thức Krüger bậc 3,
    khớp pyproj tới mức milimét trong phạm vi Đà Nẵng (đã đối soát ở bản trước).
  - trong_da_giac(): điểm có nằm trong một đa giác (có thể nhiều mảnh, có lỗ) hay không – thuật toán "bắn tia".
  - kc_toi_bien(): khoảng cách ngắn nhất từ một điểm tới đường biên đa giác.
"""
import math  # hàm toán học cơ bản: sin, cos, radians...

import numpy as np  # tính toán trên mảng số


def utm49n(lon, lat):  # đổi (kinh độ, vĩ độ) sang (x, y) mét của UTM 49N; nhận số đơn hoặc mảng
    lon = np.asarray(lon, dtype=float)  # chuyển kinh độ thành mảng số thực
    lat = np.asarray(lat, dtype=float)  # chuyển vĩ độ thành mảng số thực
    a, f, k0, lon0 = 6378137.0, 1 / 298.257223563, 0.9996, 111.0  # bán trục lớn, độ dẹt WGS84, hệ số tỷ lệ, kinh tuyến trục múi 49
    n = f / (2 - f)  # độ dẹt thứ ba
    A = a / (1 + n) * (1 + n ** 2 / 4 + n ** 4 / 64)  # bán kính chỉnh hoá
    al = (n / 2 - 2 * n ** 2 / 3 + 5 * n ** 3 / 16,  # hệ số chuỗi Krüger thứ 1
          13 * n ** 2 / 48 - 3 * n ** 3 / 5,  # hệ số thứ 2
          61 * n ** 3 / 240)  # hệ số thứ 3
    phi = np.radians(lat)  # vĩ độ đổi sang radian
    lam = np.radians(lon - lon0)  # chênh kinh độ so với kinh tuyến trục, đổi sang radian
    c = 2 * math.sqrt(n) / (1 + n)  # hằng số phụ
    t = np.sinh(np.arctanh(np.sin(phi)) - c * np.arctanh(c * np.sin(phi)))  # vĩ độ bảo giác (dạng sinh)
    xi = np.arctan2(t, np.cos(lam))  # toạ độ trung gian theo hướng bắc
    eta = np.arctanh(np.sin(lam) / np.sqrt(1 + t * t))  # toạ độ trung gian theo hướng đông
    x = 500000 + k0 * A * (eta + sum(al[j] * np.cos(2 * (j + 1) * xi) * np.sinh(2 * (j + 1) * eta) for j in range(3)))  # toạ độ đông (cộng 500 km)
    y = k0 * A * (xi + sum(al[j] * np.sin(2 * (j + 1) * xi) * np.cosh(2 * (j + 1) * eta) for j in range(3)))  # toạ độ bắc
    return x, y  # trả về cặp (x, y) đơn vị mét


def utm49n_cap(lon, lat):  # phiên bản cho một điểm, dùng làm UDF trong Spark: trả (x, y) hoặc None nếu thiếu toạ độ
    if lon is None or lat is None:  # thiếu kinh độ hoặc vĩ độ
        return None  # không đổi được
    x, y = utm49n(lon, lat)  # gọi hàm mảng ở trên với một điểm
    return float(x), float(y)  # đổi về số thực Python để Spark nhận


def cac_vong(geometry: dict) -> list:  # tách hình học GeoJSON (Polygon / MultiPolygon) thành danh sách các vòng toạ độ
    if geometry["type"] == "Polygon":  # đa giác một mảnh
        polys = [geometry["coordinates"]]  # gói thành danh sách một phần tử
    elif geometry["type"] == "MultiPolygon":  # đa giác nhiều mảnh
        polys = geometry["coordinates"]  # đã là danh sách các mảnh
    else:  # kiểu khác (điểm, đường) không phải vùng
        return []  # không có vòng nào
    out = []  # danh sách kết quả
    for poly in polys:  # duyệt từng mảnh
        for ring in poly:  # mỗi mảnh gồm vòng ngoài và các vòng lỗ
            out.append(np.asarray(ring, dtype=float)[:, :2])  # giữ hai cột (kinh độ, vĩ độ)
    return out  # trả danh sách mảng N x 2


def trong_da_giac(x: float, y: float, vongs: list) -> bool:  # điểm (x, y) có nằm trong vùng do các vòng tạo nên không
    dem = 0  # số lần tia cắt biên
    for v in vongs:  # duyệt từng vòng (cả vòng ngoài lẫn lỗ)
        x1, y1 = v[:-1, 0], v[:-1, 1]  # đầu mỗi cạnh
        x2, y2 = v[1:, 0], v[1:, 1]  # cuối mỗi cạnh
        cat = (y1 > y) != (y2 > y)  # cạnh có vắt qua đường ngang y của điểm không
        with np.errstate(divide="ignore", invalid="ignore"):  # bỏ cảnh báo chia cho 0 (cạnh nằm ngang đã bị loại bởi `cat`)
            xc = x1 + (y - y1) * (x2 - x1) / (y2 - y1)  # hoành độ giao điểm của cạnh với đường ngang
        dem += int(np.sum(cat & (xc > x)))  # đếm giao điểm nằm bên phải điểm (tia bắn sang phải)
    return dem % 2 == 1  # số lần cắt lẻ = ở trong (quy tắc chẵn–lẻ, tự xử lý được lỗ)


def kc_toi_bien(x: float, y: float, vongs: list) -> float:  # khoảng cách ngắn nhất từ điểm tới các cạnh biên (cùng đơn vị với toạ độ)
    tot = np.inf  # khởi tạo bằng vô cùng
    for v in vongs:  # duyệt từng vòng
        ax, ay = v[:-1, 0], v[:-1, 1]  # đầu cạnh
        bx, by = v[1:, 0], v[1:, 1]  # cuối cạnh
        dx, dy = bx - ax, by - ay  # véc-tơ cạnh
        L2 = dx * dx + dy * dy  # bình phương độ dài cạnh
        with np.errstate(divide="ignore", invalid="ignore"):  # bỏ cảnh báo khi cạnh dài 0
            t = np.clip(((x - ax) * dx + (y - ay) * dy) / L2, 0, 1)  # vị trí hình chiếu của điểm trên cạnh, kẹp trong [0, 1]
        t = np.nan_to_num(t)  # cạnh dài 0 thì lấy đầu cạnh
        d = np.hypot(ax + t * dx - x, ay + t * dy - y)  # khoảng cách từ điểm tới điểm gần nhất trên từng cạnh
        tot = min(tot, float(d.min()))  # giữ giá trị nhỏ nhất
    return tot  # trả khoảng cách
