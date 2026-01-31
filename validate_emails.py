#!/usr/bin/env python3
"""Validate email addresses against MongoDB toxicdocs data.

For each email in email_network.json, check if it appears in any MongoDB document.
Emails with 0 hits are likely OCR errors. For those, find the closest match
in the dataset using fuzzy matching.

Run on EC2 where MongoDB is available.
"""

import json
import re
import sys
from collections import defaultdict
from pymongo import MongoClient

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "edgifoia"
INPUT = "public/email_network.json"

def main():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]

    data = json.load(open(INPUT))
    nodes = data['nodes']

    print(f"Total nodes: {len(nodes)}", file=sys.stderr)

    # Test each email - just check if it appears in any document
    no_hits = []
    has_hits = []

    for i, node in enumerate(nodes):
        email = node['id']
        count = node['count']
        name = node['name']

        if i % 200 == 0:
            print(f"  Checking {i}/{len(nodes)}...", file=sys.stderr)

        # Escape for regex and search
        escaped = re.escape(email)
        result = db.documents.find_one(
            {'text': {'$regex': escaped, '$options': 'i'}},
            {'_id': 1}
        )

        if result:
            has_hits.append(email)
        else:
            no_hits.append((email, name, count))

    client.close()

    print(f"\nEmails with hits: {len(has_hits)}", file=sys.stderr)
    print(f"Emails with NO hits: {len(no_hits)}", file=sys.stderr)

    # Output the no-hit emails as JSON
    no_hits.sort(key=lambda x: -x[2])
    output = []
    for email, name, count in no_hits:
        output.append({"email": email, "name": name, "count": count})

    json.dump(output, sys.stdout, indent=2)
    print()

if __name__ == "__main__":
    main()
