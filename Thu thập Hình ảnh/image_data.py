# collector.py
import os, io, hashlib
import requests
import boto3
from sqlalchemy import create_engine, Table, Column, Integer, Text, MetaData, ARRAY
from config import *

# 1. Thiết lập S3 client
s3 = boto3.client(
    "s3",
    region_name = AWS_REGION,
    aws_access_key_id = AWS_ACCESS_KEY,
    aws_secret_access_key = AWS_SECRET_KEY,
)

# 2. Thiết lập DB với SQLAlchemy
DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(DATABASE_URL)
meta = MetaData()
images = Table(
    "images", meta,
    Column("id",       Integer, primary_key=True),
    Column("filename", Text,    nullable=False),
    Column("s3_url",   Text,    nullable=False),
    Column("source",   Text,    nullable=False),
    Column("tags",     ARRAY(Text)),
    Column("unsplash_id", Text, unique=True),
)

# 3. Hàm check duplicate bằng unsplash_id
def exists_in_db(unsplash_id):
    with engine.connect() as conn:
        res = conn.execute(
            images.select().where(images.c.unsplash_id == unsplash_id)
        ).first()
        return res is not None

# 4. Hàm upload file lên S3
def upload_to_s3(image_bytes, key):
    s3.upload_fileobj(io.BytesIO(image_bytes), S3_BUCKET, key)
    url = f"https://{S3_BUCKET}.s3.{AWS_REGION}.amazonaws.com/{key}"
    return url

# 5. Thu thập từ Unsplash
def fetch_and_store():
    url = "https://api.unsplash.com/photos/random"
    headers = {"Authorization": f"Client-ID {UNSPLASH_ACCESS_KEY}"}
    params = {"query": UNSPLASH_QUERY, "count": UNSPLASH_COUNT}
    resp = requests.get(url, headers=headers, params=params)
    resp.raise_for_status()
    photos = resp.json()

    for p in photos:
        uid = p["id"]
        if exists_in_db(uid):
            print(f"Đã có ảnh {uid}, bỏ qua.")
            continue

        img_url = p["urls"]["full"]
        tags    = [t["title"] for t in p.get("tags", [])]

        # Tải ảnh về
        img_resp = requests.get(img_url)
        img_resp.raise_for_status()
        data = img_resp.content

        # Tạo filename chuẩn: hash của nội dung
        md5 = hashlib.md5(data).hexdigest()
        ext = img_url.split("?")[0].split(".")[-1]  # jpg/png
        filename = f"{md5}.{ext}"
        s3_key   = f"{UNSPLASH_QUERY}/{filename}"

        # Upload lên S3 & lấy URL
        s3_url = upload_to_s3(data, s3_key)
        print(f"Uploaded {filename} → {s3_url}")

        # Lưu metadata vào DB
        with engine.connect() as conn:
            conn.execute(
                images.insert().values(
                    filename=filename,
                    s3_url=s3_url,
                    source="unsplash",
                    tags=tags,
                    unsplash_id=uid
                )
            )
        print(f"Inserted metadata for {uid} vào DB.")

if __name__ == "__main__":
    fetch_and_store()
