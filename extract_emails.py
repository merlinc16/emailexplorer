#!/usr/bin/env python3
"""
Extract email correspondence network from MongoDB toxic_docs collection.
Outputs a JSON file suitable for D3.js force-directed graph visualization.
"""

import re
import json
import argparse
import os
from collections import defaultdict
from pymongo import MongoClient

# Email regex - requires at least 2 chars before @
EMAIL_REGEX = re.compile(r'[a-zA-Z0-9][a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.(com|net|org|edu|gov|mil|co|io|me|info|biz)', re.IGNORECASE)

# Patterns for extracting From/To/CC
FROM_PATTERNS = [
    re.compile(r'From:\s*([^<\n]*?)\s*<?([a-zA-Z0-9][a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-z]{2,})>?', re.IGNORECASE),
    re.compile(r'Author:\s*([a-zA-Z0-9][a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-z]{2,})', re.IGNORECASE),
    re.compile(r'Sent by:\s*([a-zA-Z0-9][a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-z]{2,})', re.IGNORECASE),
]

TO_PATTERNS = [
    re.compile(r'(?:^|\n)\s*To:\s*([^<\n]*?)\s*<?([a-zA-Z0-9][a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-z]{2,})>?', re.IGNORECASE),
    re.compile(r'(?:^|\n)\s*TO:\s*([^<\n;]*?)\s*<?([a-zA-Z0-9][a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-z]{2,})>?', re.IGNORECASE),
]

CC_PATTERNS = [
    re.compile(r'(?:^|\n)\s*CC?:\s*([^<\n]*?)\s*<?([a-zA-Z0-9][a-zA-Z0-9._-]+@[a-zA-Z0-9.-]+\.[a-z]{2,})>?', re.IGNORECASE),
]


def is_valid_email(email):
    """Validate email has reasonable format."""
    if not email or '@' not in email:
        return False
    local_part = email.split('@')[0]
    # Require at least 2 chars in local part
    if len(local_part) < 2:
        return False
    # Filter out obvious garbage
    if local_part.isdigit():
        return False
    return True


def extract_name_from_email(email):
    """Convert email to display name."""
    local_part = email.split('@')[0]
    domain = email.split('@')[1].lower() if '@' in email else ''

    # Normalize domain for checking
    norm_domain = normalize_domain(domain)

    # Replace dots and underscores with spaces, remove extra dots
    local_clean = re.sub(r'\.+', '.', local_part)  # Collapse multiple dots
    local_clean = local_clean.strip('.')  # Remove leading/trailing dots
    name = re.sub(r'[._]', ' ', local_clean)
    parts = [p for p in name.split() if p]  # Filter empty parts

    # For EPA emails, format is typically lastname.firstname - reverse it
    if norm_domain == 'epa.gov' and len(parts) == 2:
        # Check if both parts look like name parts (not numbers, reasonable length)
        if (parts[0].replace('-', '').replace("'", '').isalpha() and
            parts[1].replace('-', '').replace("'", '').isalpha() and
            len(parts[0]) > 1 and len(parts[1]) > 1):
            # Reverse: beck nancy -> Nancy Beck
            parts = [parts[1], parts[0]]

    return ' '.join(parts).title()


def fix_reversed_name(name):
    """
    Fix names in 'Last, First' format to 'First Last'.
    Common in government email systems.
    """
    if not name:
        return None

    name = name.strip().strip('"\'')

    # Reject if it looks like email chain garbage
    garbage_indicators = ['sent:', 'mailto:', 'subject:', 'to:', 'from:', 'cc:',
                          'am to:', 'pm to:', 'monday', 'tuesday', 'wednesday',
                          'thursday', 'friday', 'saturday', 'sunday', '@',
                          'january', 'february', 'march', 'april', 'may', 'june',
                          'july', 'august', 'september', 'october', 'november', 'december']
    name_lower = name.lower()
    for indicator in garbage_indicators:
        if indicator in name_lower:
            return None

    # Reject if too long (real names are rarely > 40 chars)
    if len(name) > 40:
        return None

    # Reject if it has numbers
    if any(c.isdigit() for c in name):
        return None

    # Check for "Last, First" pattern (comma indicates reversed)
    if ',' in name:
        parts = [p.strip() for p in name.split(',', 1)]
        if len(parts) == 2 and parts[0] and parts[1]:
            # Get just first word of each part
            first_part = parts[1].split()[0] if parts[1].split() else ''
            last_part = parts[0].split()[-1] if parts[0].split() else ''

            # Only flip if parts look like names (mostly alpha, reasonable length)
            if (first_part.replace('.', '').replace('-', '').replace("'", '').isalpha() and
                last_part.replace('.', '').replace('-', '').replace("'", '').isalpha() and
                2 <= len(first_part) <= 20 and 2 <= len(last_part) <= 20):
                # Return just first name and last name
                first = parts[1].split()[0]
                last = parts[0].split()[0] if len(parts[0].split()) == 1 else parts[0]
                return f"{first} {last}".title()

    # No comma - check if it's a reasonable name
    words = name.split()
    if 1 <= len(words) <= 4:
        # Check all words look like name parts
        if all(w.replace('.', '').replace('-', '').replace("'", '').isalpha() and len(w) >= 2 for w in words):
            return name.title()

    return None


def extract_display_name(text, email):
    """
    Extract the display name for an email from the From/To field text.
    Looks for patterns like:
    - From: "Graham, Amy" <graham.amy@epa.gov>
    - From: Graham, Amy <graham.amy@epa.gov>
    - From: Amy Graham <amy.graham@epa.gov>
    """
    if not text or not email:
        return None

    # Pattern to find name before email
    # Match: Name <email> or "Name" <email>
    patterns = [
        # "Last, First" <email> or 'Last, First' <email>
        re.compile(r'["\']([^"\']+)["\']?\s*<?' + re.escape(email.split('@')[0]) + r'@', re.IGNORECASE),
        # Last, First <email> (no quotes)
        re.compile(r'(?:From|To|Cc):\s*([^<\n]+?)\s*<?' + re.escape(email.split('@')[0]) + r'@', re.IGNORECASE),
    ]

    for pattern in patterns:
        match = pattern.search(text)
        if match:
            name = match.group(1).strip()
            # Filter out garbage (too long, has weird chars, etc.)
            if name and len(name) < 50 and not re.search(r'[@/\\]', name):
                return fix_reversed_name(name)

    return None


def get_domain(email):
    """Extract domain from email."""
    return email.split('@')[1].lower() if '@' in email else ''


def normalize_domain(domain):
    """Fix common OCR errors in domains."""
    domain = domain.lower().strip()

    # Remove leading dots
    domain = domain.lstrip('.')

    # EPA OCR errors
    epa_errors = [
        'epa.govl', 'epa.qov', 'epa.qovl', 'epa.goy', 'epa.aov', 'epa.aovl',
        'epa.gqv', 'epa.rov', 'epa.rovl', 'epa.fiov', 'epa.giov', 'epa.g0v',
        'ep3.gov', 'ep3.govl', 'cpa.gov', 'cpa.govl', 'cp3.gov', 'epa.qoy',
        'epa.aoyl', 'epa.goyl', 'epa.gov1', 'epa.go v', 'epamail.epa.gov',
        'epa.flov', 'epa.gqvl', 'epa.qqv', 'epa.gq', 'epa.govcmai', 'epa.eov',
        'epa.gqyl', 'epa.rgv', 'epa.go', 'epa.govemai', 'epa.oov', 'epa.oovl',
        'epa..gov', 'epa.uo', 'epa.qo', 'epa.ggy', 'epa.qqvl', 'epa.gqy',
        'epa.gm', 'epa.govt', 'epa.ggv', 'epa.rqv', 'epa.qqyl', 'epa.sov',
        'epa.flovl', 'epa.rovj', 'epa.gqvi', 'epa.jtov', 'epa.goto', 'epa.rqy',
        'epa.governai', 'epa.aoy', 'epa.ciov', 'epa.qoyl', 'epa.qovy', 'epa.ggyl',
        'epa.govj', 'epa..gqv', 'epa.rev', 'epa.gev', 'epa.p.ov', 'epa.g.qy',
        'epa.gow', 'epa.qqy', 'epa.qol', '-epa.gov', '1epa.gov', '1lepa.gov',
        '11epa.gov', 'gepa.gov', 'jepa.gov', 'epamail.gov',
        'domino.epamail.epa.gov', 'usepa.onmicrosoft.com'
    ]
    if domain in epa_errors:
        return 'epa.gov'

    # State EPA domains
    if domain in ['iepa.gov', 'ilepa.gov']:  # Illinois EPA
        return 'iepa.gov'
    if domain == 'calepa.ca.gov':  # California EPA
        return 'calepa.ca.gov'

    # Other common OCR fixes
    ocr_fixes = {
        # BLM
        'b1m.gov': 'blm.gov',
        # General .gov OCR errors (l instead of nothing at end)
        'govl': 'gov',
    }

    for error, fix in ocr_fixes.items():
        if domain == error:
            return fix

    # Fix trailing 'l' on .gov domains (govl -> gov)
    if domain.endswith('.govl'):
        return domain[:-1]  # Remove the 'l'

    # Fix .qov -> .gov
    if domain.endswith('.qov'):
        return domain[:-3] + 'gov'

    # Fix .aov -> .gov
    if domain.endswith('.aov'):
        return domain[:-3] + 'gov'

    # Fix .goy -> .gov
    if domain.endswith('.goy'):
        return domain[:-3] + 'gov'

    # Fix .rov -> .gov
    if domain.endswith('.rov'):
        return domain[:-3] + 'gov'

    return domain


def normalize_email(email):
    """Normalize an email address: fix domain OCR errors, replace hyphens with
    periods in .gov local parts (gov systems don't use hyphens in addresses)."""
    if '@' not in email:
        return email
    local, domain = email.split('@', 1)
    domain = normalize_domain(domain)
    if domain.endswith('.gov'):
        local = local.replace('-', '.')
    return f"{local}@{domain}"


def canonicalize_email(email):
    """
    Canonicalize email to handle reversed names like scott.pruitt vs pruitt.scott.
    Returns a canonical form by sorting the name parts alphabetically.
    """
    if '@' not in email:
        return email

    local_part, domain = email.split('@', 1)

    # Split local part by common separators (., _, -)
    parts = re.split(r'[._-]', local_part.lower())

    # Filter out empty parts and single chars
    parts = [p for p in parts if len(p) > 1]

    if len(parts) >= 2:
        # Sort parts alphabetically to canonicalize
        # e.g., "pruitt.scott" and "scott.pruitt" both become "pruitt.scott"
        sorted_parts = sorted(parts)
        canonical_local = '.'.join(sorted_parts)
        return f"{canonical_local}@{domain}"

    return email


def build_email_aliases(emails):
    """
    Build a mapping from all email variations to their canonical form.
    Groups emails by domain and canonical local part.
    """
    # Group by canonical form
    canonical_groups = defaultdict(set)
    for email in emails:
        canonical = canonicalize_email(email)
        canonical_groups[canonical].add(email)

    # Build alias map: each email -> the most common variant in its group
    alias_map = {}
    for canonical, variants in canonical_groups.items():
        if len(variants) > 1:
            # Pick the variant with the "best" format (prefer first.last over last.first)
            # Use the one that appears most natural (first alphabetically tends to be first.last)
            best = min(variants)  # alphabetically first tends to be better
            for v in variants:
                alias_map[v] = best
        else:
            # Single variant, no aliasing needed
            email = list(variants)[0]
            alias_map[email] = email

    return alias_map


def parse_email_document(text):
    """Extract From, To, CC email addresses from document text."""
    if not text:
        return None

    result = {
        'from': set(),
        'to': set(),
        'cc': set(),
        'display_names': {}  # email -> display name
    }

    # Extract From addresses and display names
    for pattern in FROM_PATTERNS:
        for match in pattern.finditer(text):
            email = (match.group(2) if match.lastindex >= 2 else match.group(1)).lower()
            if is_valid_email(email):
                result['from'].add(email)
                # Try to get display name from the match
                if match.lastindex >= 2:
                    raw_name = match.group(1).strip()
                    if raw_name and len(raw_name) > 1:
                        fixed_name = fix_reversed_name(raw_name)
                        if fixed_name:
                            result['display_names'][email] = fixed_name

    # Extract To addresses
    for pattern in TO_PATTERNS:
        for match in pattern.finditer(text):
            email = (match.group(2) if match.lastindex >= 2 else match.group(1)).lower()
            if is_valid_email(email):
                result['to'].add(email)

    # Extract CC addresses
    for pattern in CC_PATTERNS:
        for match in pattern.finditer(text):
            email = (match.group(2) if match.lastindex >= 2 else match.group(1)).lower()
            if is_valid_email(email):
                result['cc'].add(email)

    # Also scan TO/CC lines more broadly for emails
    to_lines = re.findall(r'(?:To|TO):[^\n]*', text)
    cc_lines = re.findall(r'(?:Cc|CC):[^\n]*', text)

    for line in to_lines:
        for email in EMAIL_REGEX.findall(line):
            if isinstance(email, tuple):
                email = email[0] if email[0] else email[1]
        emails = EMAIL_REGEX.findall(line)
        for match in re.finditer(EMAIL_REGEX, line):
            email = match.group(0).lower()
            if is_valid_email(email):
                result['to'].add(email)

    for line in cc_lines:
        for match in re.finditer(EMAIL_REGEX, line):
            email = match.group(0).lower()
            if is_valid_email(email):
                result['cc'].add(email)

    if not result['from'] and not result['to']:
        return None

    # Normalize all extracted emails (domain OCR fixes + .gov hyphen-to-period)
    result['from'] = {normalize_email(e) for e in result['from']}
    result['to'] = {normalize_email(e) for e in result['to']}
    result['cc'] = {normalize_email(e) for e in result['cc']}
    result['display_names'] = {normalize_email(k): v for k, v in result['display_names'].items()}

    return result


def build_email_network(db, max_docs=None):
    """Build email correspondence network from MongoDB documents."""

    # Query for documents with email patterns
    query = {
        '$and': [
            {'text': {'$regex': r'From:.*@', '$options': 'i'}},
            {'text': {'$regex': r'To:.*@', '$options': 'i'}}
        ]
    }

    cursor = db.documents.find(query)
    if max_docs:
        cursor = cursor.limit(max_docs)

    # Track nodes (email addresses) and edges (correspondence)
    nodes = {}  # email -> {count, sent_count, received_count, domains, years}
    edges = defaultdict(lambda: {'weight': 0, 'years': set(), 'doc_ids': set()})  # (from, to) -> {weight, years, doc_ids}
    display_names = {}  # email -> display name (best one found)

    doc_count = 0
    email_docs = 0

    print("Processing documents...")

    for doc in cursor:
        doc_count += 1
        if doc_count % 1000 == 0:
            print(f"  Processed {doc_count} documents, found {email_docs} with emails, {len(nodes)} unique addresses...")

        text = doc.get('text', '')
        year = doc.get('year')
        hash_id = doc.get('hash_id')

        parsed = parse_email_document(text)
        if not parsed:
            continue

        email_docs += 1

        # Collect display names (prefer first valid one found for each email)
        for email, name in parsed.get('display_names', {}).items():
            if name and email not in display_names:
                # Additional validation - name shouldn't match the email local part exactly
                local_part = email.split('@')[0].lower().replace('.', ' ').replace('_', ' ')
                name_normalized = name.lower()
                # Keep if name adds info (not just reformatted email local part)
                display_names[email] = name

        from_emails = parsed['from']
        to_emails = parsed['to'] | parsed['cc']

        # Update node stats
        for email in from_emails:
            if email not in nodes:
                nodes[email] = {
                    'sent_count': 0,
                    'received_count': 0,
                    'years': set(),
                    'domains_sent_to': set()
                }
            nodes[email]['sent_count'] += len(to_emails)
            if year:
                nodes[email]['years'].add(year)
            for to_email in to_emails:
                nodes[email]['domains_sent_to'].add(get_domain(to_email))

        for email in to_emails:
            if email not in nodes:
                nodes[email] = {
                    'sent_count': 0,
                    'received_count': 0,
                    'years': set(),
                    'domains_sent_to': set()
                }
            nodes[email]['received_count'] += 1
            if year:
                nodes[email]['years'].add(year)

        # Create edges (from -> to)
        for from_email in from_emails:
            for to_email in to_emails:
                if from_email != to_email:
                    edge_key = (from_email, to_email)
                    edges[edge_key]['weight'] += 1
                    if year:
                        edges[edge_key]['years'].add(year)
                    if hash_id:
                        edges[edge_key]['doc_ids'].add(hash_id)

    print(f"\nExtraction complete:")
    print(f"  Documents processed: {doc_count}")
    print(f"  Documents with emails: {email_docs}")
    print(f"  Unique email addresses: {len(nodes)}")
    print(f"  Email connections: {len(edges)}")
    print(f"  Display names found: {len(display_names)}")

    return nodes, edges, display_names


def export_to_json(nodes, edges, display_names, output_file, min_count=1, min_weight=1):
    """Export network to D3.js-compatible JSON."""

    # Step 1: Build alias map to consolidate duplicates (e.g., pruitt.scott -> scott.pruitt)
    print("Consolidating duplicate email addresses...")
    alias_map = build_email_aliases(nodes.keys())

    # Count how many were consolidated
    unique_canonicals = len(set(alias_map.values()))
    consolidated = len(alias_map) - unique_canonicals
    print(f"  Consolidated {consolidated} duplicate addresses")

    # Step 2: Merge nodes by canonical email
    merged_nodes = {}
    for email, data in nodes.items():
        canonical = alias_map.get(email, email)
        if canonical not in merged_nodes:
            merged_nodes[canonical] = {
                'sent_count': 0,
                'received_count': 0,
                'years': set(),
                'domains_sent_to': set(),
                'aliases': set()
            }
        merged_nodes[canonical]['sent_count'] += data['sent_count']
        merged_nodes[canonical]['received_count'] += data['received_count']
        merged_nodes[canonical]['years'].update(data['years'])
        merged_nodes[canonical]['domains_sent_to'].update(data['domains_sent_to'])
        if email != canonical:
            merged_nodes[canonical]['aliases'].add(email)

    # Step 3: Merge edges by canonical emails
    merged_edges = defaultdict(lambda: {'weight': 0, 'years': set(), 'doc_ids': set()})
    for (source, target), data in edges.items():
        canonical_source = alias_map.get(source, source)
        canonical_target = alias_map.get(target, target)
        if canonical_source != canonical_target:  # Skip self-loops
            edge_key = (canonical_source, canonical_target)
            merged_edges[edge_key]['weight'] += data['weight']
            merged_edges[edge_key]['years'].update(data['years'])
            merged_edges[edge_key]['doc_ids'].update(data.get('doc_ids', set()))

    # Filter nodes by minimum activity
    filtered_nodes = {}
    for email, data in merged_nodes.items():
        total_count = data['sent_count'] + data['received_count']
        if total_count >= min_count:
            filtered_nodes[email] = data

    # Build merged display names (canonical email -> best display name)
    merged_display_names = {}
    for email, name in display_names.items():
        canonical = alias_map.get(email, email)
        if canonical not in merged_display_names:
            merged_display_names[canonical] = name

    # Build node list
    node_list = []
    node_ids = set()

    for email, data in filtered_nodes.items():
        # Use display name if available, otherwise fall back to email-derived name
        name = merged_display_names.get(email, extract_name_from_email(email))
        node_list.append({
            'id': email,
            'name': name,
            'domain': normalize_domain(get_domain(email)),
            'sent': data['sent_count'],
            'received': data['received_count'],
            'count': data['sent_count'] + data['received_count'],
            'years': sorted(list(data['years'])),
            'domain_count': len(data['domains_sent_to'])
        })
        node_ids.add(email)

    # Filter edges - both endpoints must exist and meet weight threshold
    edge_list = []
    for (source, target), data in merged_edges.items():
        if source in node_ids and target in node_ids and data['weight'] >= min_weight:
            edge_list.append({
                'source': source,
                'target': target,
                'weight': data['weight'],
                'years': sorted(list(data['years'])),
                'doc_ids': sorted(list(data.get('doc_ids', set())))
            })

    # Sort nodes by count descending
    node_list.sort(key=lambda x: x['count'], reverse=True)

    # Gather domain statistics
    domains = defaultdict(int)
    for node in node_list:
        domains[node['domain']] += 1

    top_domains = sorted(domains.items(), key=lambda x: x[1], reverse=True)[:50]

    # Build output
    output = {
        'stats': {
            'nodes': len(node_list),
            'edges': len(edge_list),
            'top_domains': [{'domain': d, 'count': c} for d, c in top_domains]
        },
        'nodes': node_list,
        'edges': edge_list
    }

    print(f"\nExporting to {output_file}...")
    print(f"  Nodes (min_count={min_count}): {len(node_list)}")
    print(f"  Edges (min_weight={min_weight}): {len(edge_list)}")

    with open(output_file, 'w') as f:
        json.dump(output, f)

    print(f"  File size: {os.path.getsize(output_file) / 1024 / 1024:.1f} MB")

    return output


def main():
    import os

    parser = argparse.ArgumentParser(description='Extract email network from MongoDB')
    parser.add_argument('--mongo-uri', default='mongodb://localhost:27017', help='MongoDB URI')
    parser.add_argument('--db', default='toxic_docs', help='Database name')
    parser.add_argument('--output', default='email_network.json', help='Output JSON file')
    parser.add_argument('--max-docs', type=int, help='Maximum documents to process')
    parser.add_argument('--min-count', type=int, default=1, help='Minimum email activity to include node')
    parser.add_argument('--min-weight', type=int, default=1, help='Minimum edge weight to include')

    args = parser.parse_args()

    # Connect to MongoDB
    print(f"Connecting to MongoDB at {args.mongo_uri}...")
    client = MongoClient(args.mongo_uri)
    db = client[args.db]

    # Build network
    nodes, edges, display_names = build_email_network(db, args.max_docs)

    # Export to JSON
    export_to_json(nodes, edges, display_names, args.output, args.min_count, args.min_weight)

    print("\nDone!")


if __name__ == '__main__':
    main()
