#!/usr/bin/env python3
"""Query MongoDB for all documents containing both email addresses,
download their PDFs from CDN, zip them, upload to S3,
and return a presigned download URL.

Usage: fetch_correspondence.py <json-file>
  The JSON file should contain: {"email1": "...", "email2": "...", "name1": "...", "name2": "..."}
"""

import sys
import os
import re
import json
import time
import tempfile
import zipfile
import urllib.request
import urllib.error

import boto3
from botocore.exceptions import ClientError
from pymongo import MongoClient

DOWNLOAD_DELAY = 0.05  # seconds between CDN requests
S3_BUCKET = "edgizips"
S3_PREFIX = "correspondence"
PRESIGN_EXPIRY = 3600  # 1 hour
MAX_DOCS = 500  # safety cap

CDN_BASE = "https://cdn.toxicdocs.org"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "edgifoia"


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def build_pdf_url(hash_id):
    prefix = hash_id[:2]
    return f"{CDN_BASE}/{prefix}/{hash_id}/{hash_id}.pdf"


def build_email_regex(email):
    """Build a regex that matches this email with OCR domain variants.

    For example, bennett.tate@epa.gov matches:
      bennett.tate@epa.gov
      bennett.tate@epa.govl
      bennett.tate@epamail.epa.gov
    """
    local, domain = email.split('@', 1)
    local_escaped = re.escape(local)

    # Use base domain (last two parts) to be flexible with subdomains/OCR
    domain_parts = domain.split('.')
    if len(domain_parts) >= 2:
        base = '.'.join(domain_parts[-2:])
    else:
        base = domain
    base_escaped = re.escape(base)

    return f'{local_escaped}@[a-zA-Z0-9._-]*{base_escaped}'


def query_documents(email1, email2):
    """Query MongoDB for all documents containing both email addresses."""
    regex1 = build_email_regex(email1)
    regex2 = build_email_regex(email2)

    log(f"MongoDB regex1: {regex1}")
    log(f"MongoDB regex2: {regex2}")

    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    query = {
        '$and': [
            {'text': {'$regex': regex1, '$options': 'i'}},
            {'text': {'$regex': regex2, '$options': 'i'}}
        ]
    }

    cursor = db.documents.find(query, {'hash_id': 1}).limit(MAX_DOCS)
    hash_ids = []
    for doc in cursor:
        hid = doc.get('hash_id')
        if hid:
            hash_ids.append(str(hid))

    client.close()
    return hash_ids


def main():
    if len(sys.argv) != 2:
        print(json.dumps({"success": False, "error": "Usage: fetch_correspondence.py <json-file>"}))
        sys.exit(1)

    with open(sys.argv[1]) as f:
        params = json.load(f)

    email1 = params.get("email1", "")
    email2 = params.get("email2", "")
    name1 = params.get("name1", "person1")
    name2 = params.get("name2", "person2")

    if not email1 or not email2:
        print(json.dumps({"success": False, "error": "email1 and email2 are required"}))
        return

    log(f"Finding correspondence: {email1} <-> {email2}")

    # Query MongoDB for all matching documents
    hash_ids = query_documents(email1, email2)
    log(f"Found {len(hash_ids)} documents in MongoDB")

    if not hash_ids:
        print(json.dumps({"success": True, "doc_count": 0, "download_url": None}))
        return

    # Check S3 for existing zip (keyed by sorted email pair)
    s3_key = f"{S3_PREFIX}/{'___'.join(sorted([email1, email2]))}.zip"
    try:
        s3 = boto3.client('s3')
        s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': S3_BUCKET, 'Key': s3_key},
            ExpiresIn=PRESIGN_EXPIRY
        )
        log("Found existing zip on S3")
        print(json.dumps({
            "success": True,
            "doc_count": len(hash_ids),
            "download_url": url,
            "cached": True
        }))
        return
    except ClientError:
        pass

    # Download PDFs and create zip
    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = os.path.join(tmpdir, "correspondence.zip")
        pdf_count = 0

        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zf:
            for i, hash_id in enumerate(hash_ids):
                pdf_url = build_pdf_url(hash_id)
                log(f"[{i+1}/{len(hash_ids)}] {hash_id}")
                try:
                    req = urllib.request.Request(pdf_url, headers={
                        "User-Agent": "EmailExplorer/1.0"
                    })
                    with urllib.request.urlopen(req, timeout=30) as resp:
                        pdf_data = resp.read()
                    zf.writestr(f"{hash_id}.pdf", pdf_data)
                    pdf_count += 1
                except (urllib.error.URLError, urllib.error.HTTPError, OSError) as e:
                    log(f"  Failed: {e}")

                if i < len(hash_ids) - 1:
                    time.sleep(DOWNLOAD_DELAY)

        log(f"Zipped {pdf_count} PDFs")

        if pdf_count == 0:
            print(json.dumps({"success": True, "doc_count": 0, "download_url": None}))
            return

        # Upload to S3
        log(f"Uploading to s3://{S3_BUCKET}/{s3_key}")
        try:
            s3 = boto3.client('s3')
            s3.upload_file(zip_path, S3_BUCKET, s3_key)
            url = s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': S3_BUCKET, 'Key': s3_key},
                ExpiresIn=PRESIGN_EXPIRY
            )
            log("Upload complete")
            print(json.dumps({
                "success": True,
                "doc_count": pdf_count,
                "download_url": url
            }))
        except ClientError as e:
            log(f"S3 error: {e}")
            print(json.dumps({"success": False, "error": str(e)}))


if __name__ == "__main__":
    main()
