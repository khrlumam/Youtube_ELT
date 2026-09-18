from datetime import datetime, timedelta

def parse_duration(duration_str):

    duration_str = duration_str.replace("PT", "")

    components = ["D", "H", "M", "S"]

    # Inisialisasi dictionary untuk menyimpan nilai durasi
    values = {"D": 0, "H": 0, "M": 0, "S": 0}

    # Memeriksa setiap komponen durasi dan mengekstrak nilainya
    for component in components:
        # Jika komponen ditemukan dalam string durasi, ekstrak nilainya dan simpan dalam dictionary
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    total_duration = timedelta(
        days=values["D"],
        hours=values["H"],
        minutes=values["M"],
        seconds=values["S"]
    )

    return total_duration

def transform_data(row):
    duration_td = parse_duration(row["Duration"])
    # Mengubah durasi dari format ISO 8601 menjadi format TIME (HH:MM:SS) dan menambahkan kolom Video_Type berdasarkan durasi
    row["Duration"] = (datetime.min + duration_td).time()
    # Menambahkan kolom Video_Type dengan nilai "Shorts" jika durasi kurang dari 60 detik, dan "Normal" jika durasi 60 detik atau lebih
    row["Video_Type"] = "Shorts" if duration_td.total_seconds() <= 60 else "Normal"

    return row