#!/usr/bin/env python3.9
"""Flask server for Email Explorer - replaces server.js on EC2."""
import os
import re
import json
import time
import tempfile
import zipfile
import urllib.request
import urllib.error

from flask import Flask, request, jsonify, send_from_directory
import boto3
from botocore.exceptions import ClientError
from pymongo import MongoClient

app = Flask(__name__, static_folder="public", static_url_path="")

DOWNLOAD_DELAY = 0.05
S3_BUCKET = "edgizips"
S3_PREFIX = "correspondence"
PRESIGN_EXPIRY = 3600
MAX_DOCS = 500
CDN_BASE = "https://cdn.toxicdocs.org"

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "test")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "edgifoia")


def build_email_regex(email):
    local, domain = email.split("@", 1)
    local_escaped = re.escape(local)
    domain_parts = domain.split(".")
    if len(domain_parts) >= 2:
        base = ".".join(domain_parts[-2:])
    else:
        base = domain
    base_escaped = re.escape(base)
    return f"{local_escaped}@[a-zA-Z0-9._-]*{base_escaped}"


def build_aliases_regex(aliases):
    """Build a single regex that matches any of the email aliases."""
    patterns = [build_email_regex(a) for a in aliases if '@' in a]
    if not patterns:
        return None
    if len(patterns) == 1:
        return patterns[0]
    return '(?:' + '|'.join(patterns) + ')'


def build_pdf_url(hash_id):
    return f"{CDN_BASE}/{hash_id[:2]}/{hash_id}/{hash_id}.pdf"


@app.route("/")
def index():
    return send_from_directory("public", "index.html")


@app.route("/api/fetch-correspondence", methods=["POST"])
def fetch_correspondence():
    data = request.get_json()
    email1 = data.get("email1", "")
    email2 = data.get("email2", "")
    aliases1 = data.get("aliases1")
    aliases2 = data.get("aliases2")
    if not email1 or not email2:
        return jsonify(success=False, error="email1 and email2 are required"), 400

    # Use aliases if provided (to match OCR variants in document text)
    if aliases1 and len(aliases1) > 1:
        regex1 = build_aliases_regex(aliases1)
    else:
        regex1 = build_email_regex(email1)

    if aliases2 and len(aliases2) > 1:
        regex2 = build_aliases_regex(aliases2)
    else:
        regex2 = build_email_regex(email2)

    app.logger.info(f"Querying: {email1} <-> {email2}")
    if aliases1:
        app.logger.info(f"  aliases1: {aliases1}")
    if aliases2:
        app.logger.info(f"  aliases2: {aliases2}")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLLECTION]
    query = {"$and": [
        {"text": {"$regex": regex1, "$options": "i"}},
        {"text": {"$regex": regex2, "$options": "i"}},
    ]}
    cursor = coll.find(query, {"hash_id": 1}).limit(MAX_DOCS)
    hash_ids = [str(doc["hash_id"]) for doc in cursor if doc.get("hash_id")]
    client.close()

    app.logger.info(f"Found {len(hash_ids)} documents")

    if not hash_ids:
        return jsonify(success=True, doc_count=0, download_url=None)

    s3_key = f"{S3_PREFIX}/{'___'.join(sorted([email1, email2]))}.zip"
    try:
        s3 = boto3.client("s3")
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        url = s3.generate_presigned_url(
            "get_object", Params={"Bucket": S3_BUCKET, "Key": s3_key},
            ExpiresIn=PRESIGN_EXPIRY)
        return jsonify(success=True, doc_count=len(hash_ids), download_url=url, cached=True)
    except ClientError:
        pass

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "correspondence.zip")
        pdf_count = 0
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for i, hid in enumerate(hash_ids):
                try:
                    req = urllib.request.Request(build_pdf_url(hid),
                        headers={"User-Agent": "EmailExplorer/1.0"})
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        zf.writestr(f"{hid}.pdf", resp.read())
                    pdf_count += 1
                except Exception as e:
                    app.logger.warning(f"Failed {hid}: {e}")
                if i < len(hash_ids) - 1:
                    time.sleep(DOWNLOAD_DELAY)

        if pdf_count == 0:
            return jsonify(success=True, doc_count=0, download_url=None)

        try:
            s3 = boto3.client("s3")
            s3.upload_file(zip_path, S3_BUCKET, s3_key)
            url = s3.generate_presigned_url(
                "get_object", Params={"Bucket": S3_BUCKET, "Key": s3_key},
                ExpiresIn=PRESIGN_EXPIRY)
            return jsonify(success=True, doc_count=pdf_count, download_url=url)
        except ClientError as e:
            return jsonify(success=False, error=str(e)), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3001)
