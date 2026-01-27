#!/usr/bin/env python3
"""
Deduplicate email network nodes in email_network.json.

Applies deduplication in layers from safest to most aggressive,
merges duplicate nodes and edges, and writes the cleaned result.

Usage:
    python dedup_network.py                          # Run full dedup, overwrite JSON
    python dedup_network.py --dry-run                # Print stats only, no write
    python dedup_network.py --dry-run --report       # Print merge groups
    python dedup_network.py --no-fuzzy               # Skip Layer 4
    python dedup_network.py --output cleaned.json    # Write to different file
"""

import argparse
import json
import os
import re
import shutil
import sys
from collections import defaultdict

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_INPUT = os.path.join(SCRIPT_DIR, "public", "email_network.json")

# EPA OCR error domains (from extract_emails.py lines 174-189)
EPA_ERROR_DOMAINS = {
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
    'domino.epamail.epa.gov', 'usepa.onmicrosoft.com',
}

# Other specific domain fixes
DOMAIN_FIXES = {
    'ios.doi.gov': 'doi.gov',
    'sol.doi.gov': 'doi.gov',
    '.blm.gov': 'blm.gov',
    'b1m.gov': 'blm.gov',
    'qmail.com': 'gmail.com',
    'gmial.com': 'gmail.com',
    'grnail.com': 'gmail.com',
}

# OCR character substitutions for domains
DOMAIN_OCR_CHAR_MAP = {
    'rn': 'm',
    '1': 'l',
    '3': 'a',
    '0': 'o',
}

# OCR character substitutions for local parts
LOCAL_OCR_CHAR_MAP = {
    'rn': 'm',
    'ii': 'n',
    'v': 'y',
    '1': 'l',
    '0': 'o',
    '3': 'a',
}

# Regex for garbled mailto: prefixes.
# Must match at least "mailto" (or OCR variants) to avoid false positives.
# The colon may be OCR'd as 'i', '1', 'l', or missing entirely.
# Prefix chars (r, f, n, etc.) may be prepended by OCR errors.
MAILTO_RE = re.compile(
    r'^(?:'
    r'[rfnc]?mailto[i1l:;]\s*'       # mailto: with optional stray prefix
    r'|[rfnc]?rnailto[i1l:;]\s*'     # rn→m OCR variant
    r'|[rfnc]?rnai[il1]to[i1l:;]\s*' # double OCR variant
    r'|[rfnc]?mai[il1]to[i1l:;]\s*'  # mail OCR variant
    r'|mail\.to[i1l:;]\s*'           # mail.to: (dot-separated)
    r'|[rfnc]?mailtcr\s*'            # mailtcr (garbled mailto)
    r'|[rfnc]?mai[il1]sto[i1l:;]\s*' # mailsto (extra s)
    r')',
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Levenshtein distance (stdlib-only implementation)
# ---------------------------------------------------------------------------

def levenshtein(s, t):
    """Compute Levenshtein edit distance between two strings."""
    if s == t:
        return 0
    if not s:
        return len(t)
    if not t:
        return len(s)
    # Use two-row approach for memory efficiency
    prev = list(range(len(t) + 1))
    curr = [0] * (len(t) + 1)
    for i, sc in enumerate(s, 1):
        curr[0] = i
        for j, tc in enumerate(t, 1):
            cost = 0 if sc == tc else 1
            curr[j] = min(curr[j - 1] + 1, prev[j] + 1, prev[j - 1] + cost)
        prev, curr = curr, prev
    return prev[len(t)]


def jaro_winkler(s1, s2):
    """Compute Jaro-Winkler similarity between two strings."""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    max_dist = max(len(s1), len(s2)) // 2 - 1
    if max_dist < 0:
        max_dist = 0

    s1_matches = [False] * len(s1)
    s2_matches = [False] * len(s2)

    matches = 0
    transpositions = 0

    for i in range(len(s1)):
        start = max(0, i - max_dist)
        end = min(len(s2), i + max_dist + 1)
        for j in range(start, end):
            if s2_matches[j] or s1[i] != s2[j]:
                continue
            s1_matches[i] = True
            s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i in range(len(s1)):
        if not s1_matches[i]:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (matches / len(s1) + matches / len(s2) +
            (matches - transpositions / 2) / matches) / 3

    # Winkler modification
    prefix = 0
    for i in range(min(4, min(len(s1), len(s2)))):
        if s1[i] == s2[i]:
            prefix += 1
        else:
            break

    return jaro + prefix * 0.1 * (1 - jaro)


# ---------------------------------------------------------------------------
# Layer 1: Structural Cleanup
# ---------------------------------------------------------------------------

def structural_cleanup(email):
    """Strip mailto: prefixes, leading/trailing dots, collapse double dots, lowercase."""
    email = email.strip().lower()

    # Strip garbled mailto: prefixes
    email = MAILTO_RE.sub('', email)

    # Remove angle brackets
    email = email.strip('<>').strip()

    if '@' not in email:
        return email

    local, domain = email.split('@', 1)

    # Strip leading/trailing dots from local part
    local = local.strip('.')

    # Collapse double dots
    while '..' in local:
        local = local.replace('..', '.')
    while '..' in domain:
        domain = domain.replace('..', '.')

    # Strip leading/trailing dots and hyphens from domain
    domain = domain.strip('.-')

    return f"{local}@{domain}" if local and domain else email


# ---------------------------------------------------------------------------
# Layer 2: Domain Normalization
# ---------------------------------------------------------------------------

# Pattern: any single-char substitution of 'epa' + .gov (or garbled .gov)
# e.g., 8pa.gov, 6pa.gov, eba.gov, eda.gov, eoa.gov, ega.gov, era.gov, etc.
_EPA_PATTERN = re.compile(
    r'^[ecC3a8b6wWfF]?'    # garbled or missing 'e'
    r'[pPfF]?'             # garbled or missing 'p'
    r'[aA3]?'              # garbled or missing 'a'
    r'\.gov'
)

def _is_likely_epa(domain):
    """Check if domain is likely a garbled form of epa.gov."""
    # Must end with .gov (or a suffix that's been fixed to .gov)
    if not domain.endswith('.gov'):
        return False
    host = domain[:-4]  # strip .gov
    if not host:
        return False
    # Exact 3-char host that's close to 'epa'
    if len(host) == 3:
        dist = levenshtein(host, 'epa')
        return dist <= 1
    # Hosts with extra chars from OCR (like 'efia', 'ejaa', 'eiaa', 'elaa')
    if len(host) == 4:
        # Check if removing one char gives 'epa' (edit distance 1 = insertion)
        for i in range(len(host)):
            reduced = host[:i] + host[i+1:]
            if levenshtein(reduced, 'epa') <= 1:
                return True
    return False


def normalize_domain(domain):
    """Normalize domain using EPA error list and generic OCR fixes."""
    domain = domain.lower().strip('.-')

    # Remove spaces within domain
    domain = domain.replace(' ', '')

    # EPA-specific errors (explicit list)
    if domain in EPA_ERROR_DOMAINS:
        return 'epa.gov'

    # State EPA domains - preserve
    if domain in ('iepa.gov', 'ilepa.gov'):
        return domain
    if domain == 'calepa.ca.gov':
        return domain

    # Specific domain fixes
    if domain in DOMAIN_FIXES:
        return DOMAIN_FIXES[domain]

    # Generic suffix fixes (apply all, chained)
    # Handle compound errors like .goyl -> .gov (y->v then l appended)
    for _ in range(3):  # iterate to resolve multi-step garbling
        changed = False
        if domain.endswith('.govl') or domain.endswith('.gov1') or domain.endswith('.govj') or domain.endswith('.govi'):
            domain = domain[:-1]
            changed = True
        for bad_suffix, good_suffix in [
            ('.qov', '.gov'), ('.aov', '.gov'), ('.goy', '.gov'),
            ('.rov', '.gov'), ('.sov', '.gov'), ('.eov', '.gov'),
            ('.oov', '.gov'), ('.fiov', '.gov'), ('.gow', '.gov'),
            ('.gcn', '.gov'), ('.gq', '.gov'),
            ('.eom', '.com'), ('.corn', '.com'), ('.coml', '.com'),
            ('.orq', '.org'),
        ]:
            if domain.endswith(bad_suffix):
                domain = domain[:-len(bad_suffix)] + good_suffix
                changed = True
                break
        # Strip trailing l/1/j/i from TLDs (e.g., .goyl -> .goy -> .gov)
        if not changed and len(domain) > 4:
            tld = domain.rsplit('.', 1)[-1]
            if tld.endswith(('l', '1', 'j')) and tld not in ('html', 'mil'):
                domain = domain[:-1]
                changed = True
        if not changed:
            break

    # Apply OCR char map to domain parts (hostname only, not TLD)
    parts = domain.split('.')
    if len(parts) >= 2:
        for i in range(len(parts) - 1):
            part = parts[i]
            for ocr_err, fix in DOMAIN_OCR_CHAR_MAP.items():
                part = part.replace(ocr_err, fix)
            parts[i] = part
        domain = '.'.join(parts)

    # Check EPA-specific errors again after fixes
    if domain in EPA_ERROR_DOMAINS:
        return 'epa.gov'

    # Fuzzy EPA detection for remaining .gov domains
    if _is_likely_epa(domain):
        return 'epa.gov'

    return domain


def apply_domain_normalization(email):
    """Apply domain normalization to a full email address."""
    if '@' not in email:
        return email
    local, domain = email.split('@', 1)
    domain = normalize_domain(domain)
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Layer 3: Local-Part OCR Normalization
# ---------------------------------------------------------------------------

def ocr_normalize_local(local):
    """Apply OCR character substitutions to local part."""
    result = local
    # Apply substitutions in order from longest pattern to shortest
    # to avoid partial matches (e.g., 'rn' before single-char subs)
    for ocr_err, fix in sorted(LOCAL_OCR_CHAR_MAP.items(),
                                key=lambda x: -len(x[0])):
        result = result.replace(ocr_err, fix)
    return result


def canonicalize_local(local):
    """Sort name parts alphabetically for canonical form (matching extract_emails.py)."""
    parts = re.split(r'[._\-]', local)
    parts = [p for p in parts if len(p) > 1]
    if len(parts) >= 2:
        return '.'.join(sorted(parts))
    return local


def apply_local_ocr_normalization(email):
    """Apply OCR normalization + canonical sorting to an email."""
    if '@' not in email:
        return email
    local, domain = email.split('@', 1)
    local = ocr_normalize_local(local)
    local = canonicalize_local(local)
    return f"{local}@{domain}"


# ---------------------------------------------------------------------------
# Layer 4: Fuzzy Edit-Distance Matching
# ---------------------------------------------------------------------------

class _UnionFind:
    """Weighted Union-Find with path compression."""
    def __init__(self):
        self.parent = {}
        self.rank = {}
        self.weight = {}  # tracks the best (highest count) representative

    def add(self, x, count=0):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
            self.weight[x] = count

    def find(self, x):
        if self.parent[x] != x:
            self.parent[x] = self.find(self.parent[x])
        return self.parent[x]

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        # Attach smaller rank to larger
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1
        # Keep the one with higher weight as the representative's weight
        self.weight[ra] = max(self.weight[ra], self.weight[rb])

    def groups(self):
        """Return dict: representative -> set of members."""
        result = defaultdict(set)
        for x in self.parent:
            result[self.find(x)].add(x)
        return result


def fuzzy_match_groups(nodes_by_id, alias_map, skip=False):
    """Find fuzzy matches within the same domain using edit distance."""
    if skip:
        return {}

    # Build reverse: canonical -> list of original IDs that map to it
    canonical_to_originals = defaultdict(list)
    for orig_id, canon in alias_map.items():
        canonical_to_originals[canon].append(orig_id)

    # Group unique canonicals by domain
    domain_groups = defaultdict(list)
    for canon in set(alias_map.values()):
        if '@' in canon:
            domain = canon.split('@', 1)[1]
            domain_groups[domain].append(canon)

    uf = _UnionFind()

    for domain, canonicals in domain_groups.items():
        if len(canonicals) < 2:
            continue

        # Pre-compute info for each canonical
        canon_info = []
        for c in canonicals:
            local = c.split('@', 1)[0]
            count = _total_count_for_canonical(c, canonical_to_originals, nodes_by_id)
            name = _best_name_for_canonical(c, canonical_to_originals, nodes_by_id)
            uf.add(c, count)
            canon_info.append((c, local, len(local), count, name))

        # Sort by length for efficient length-difference pruning
        canon_info.sort(key=lambda x: (x[2], x[1]))

        for i in range(len(canon_info)):
            ci, li, len_i, ci_count, ni = canon_info[i]

            for j in range(i + 1, len(canon_info)):
                cj, lj, len_j, cj_count, nj = canon_info[j]

                shorter = min(len_i, len_j)
                if shorter < 2:
                    continue
                threshold = max(1, shorter // 5)

                # Length-difference pruning (since sorted by length, once
                # difference exceeds threshold we can stop for larger j)
                if len_j - len_i > threshold:
                    break

                # Skip if already in the same set
                if uf.find(ci) == uf.find(cj):
                    continue

                dist = levenshtein(li, lj)
                if dist > threshold:
                    continue

                # Check display name similarity if both have names.
                # Only gate borderline matches (dist == threshold); for
                # closer matches the address similarity is strong enough.
                if ni and nj and dist == threshold:
                    # Use both Jaro-Winkler on full names and token overlap
                    jw = jaro_winkler(ni.lower(), nj.lower())
                    # Token overlap: compare sorted word sets
                    w1 = set(ni.lower().split())
                    w2 = set(nj.lower().split())
                    common = len(w1 & w2)
                    total = len(w1 | w2)
                    token_sim = common / total if total else 1.0
                    if jw < 0.85 and token_sim < 0.4:
                        continue

                # Check traffic similarity - avoid merging distinct high-traffic people
                if ci_count > 50 and cj_count > 50:
                    ratio = max(ci_count, cj_count) / max(1, min(ci_count, cj_count))
                    if ratio < 2:
                        if ni and nj:
                            if jaro_winkler(ni.lower(), nj.lower()) < 0.95:
                                continue
                        else:
                            continue

                uf.union(ci, cj)

    # Build merge map: for each group with >1 member, map non-representative
    # members to the representative (highest count canonical in the group)
    new_merges = {}
    for rep, members in uf.groups().items():
        if len(members) <= 1:
            continue
        # Find the member with the highest count
        best = max(members, key=lambda c: _total_count_for_canonical(
            c, canonical_to_originals, nodes_by_id))
        for m in members:
            if m != best:
                new_merges[m] = best

    return new_merges


def _best_name_for_canonical(canonical, canonical_to_originals, nodes_by_id):
    """Get the best display name among all original nodes for a canonical."""
    originals = canonical_to_originals.get(canonical, [canonical])
    best_name = ""
    for oid in originals:
        node = nodes_by_id.get(oid)
        if node and node.get("name"):
            name = node["name"]
            if len(name) > len(best_name):
                best_name = name
    return best_name


def _total_count_for_canonical(canonical, canonical_to_originals, nodes_by_id):
    """Sum total count across all original nodes for a canonical."""
    originals = canonical_to_originals.get(canonical, [canonical])
    total = 0
    for oid in originals:
        node = nodes_by_id.get(oid)
        if node:
            total += node.get("count", 0)
    return total


# ---------------------------------------------------------------------------
# Layer 5: Single-Part to Full-Name Matching
# ---------------------------------------------------------------------------

def single_to_full_name_matches(alias_map, nodes_by_id):
    """Match single-part locals (e.g. sydney@epa.gov) to full-name locals
    (e.g. hupp.sydney@epa.gov) when unambiguous."""
    # Build canonical -> total count
    canonical_to_originals = defaultdict(list)
    for orig_id, canon in alias_map.items():
        canonical_to_originals[canon].append(orig_id)

    # Get unique canonicals grouped by domain
    domain_canonicals = defaultdict(list)
    for canon in set(alias_map.values()):
        if '@' in canon:
            local, domain = canon.split('@', 1)
            domain_canonicals[domain].append((local, canon))

    new_merges = {}

    for domain, entries in domain_canonicals.items():
        # Separate single-part and multi-part locals
        singles = []
        multis = []
        for local, canon in entries:
            parts = re.split(r'[._\-]', local)
            parts = [p for p in parts if len(p) > 1]
            if len(parts) <= 1:
                singles.append((local, canon))
            else:
                multis.append((local, canon, parts))

        if not singles or not multis:
            continue

        for single_local, single_canon in singles:
            if single_canon in new_merges:
                continue

            # Find multi-part locals that contain this single part
            candidates = []
            for multi_local, multi_canon, parts in multis:
                if multi_canon in new_merges:
                    continue
                if single_local in parts:
                    count = _total_count_for_canonical(
                        multi_canon, canonical_to_originals, nodes_by_id)
                    candidates.append((multi_canon, count))

            if not candidates:
                continue

            if len(candidates) == 1:
                # Unambiguous match
                new_merges[single_canon] = candidates[0][0]
            else:
                # Pick the one with highest traffic if it's 5x any other
                candidates.sort(key=lambda x: -x[1])
                top_count = candidates[0][1]
                second_count = candidates[1][1]
                if top_count > 0 and (second_count == 0 or top_count / max(1, second_count) >= 5):
                    new_merges[single_canon] = candidates[0][0]

    return new_merges


# ---------------------------------------------------------------------------
# Layer 6: Concatenation Matching
# ---------------------------------------------------------------------------

def concatenation_matches(alias_map, nodes_by_id):
    """Match concatenated locals (bennetttate@...) to dotted forms (bennett.tate@...)."""
    canonical_to_originals = defaultdict(list)
    for orig_id, canon in alias_map.items():
        canonical_to_originals[canon].append(orig_id)

    # Get unique canonicals grouped by domain
    domain_canonicals = defaultdict(list)
    for canon in set(alias_map.values()):
        if '@' in canon:
            local, domain = canon.split('@', 1)
            domain_canonicals[domain].append((local, canon))

    # Build set of known multi-part local sets per domain for lookup
    domain_multiparts = defaultdict(dict)
    for domain, entries in domain_canonicals.items():
        for local, canon in entries:
            parts = re.split(r'[._\-]', local)
            parts = [p for p in parts if len(p) > 1]
            if len(parts) >= 2:
                # Store sorted tuple of parts -> canonical
                key = tuple(sorted(parts))
                domain_multiparts[domain][key] = canon

    new_merges = {}

    for domain, entries in domain_canonicals.items():
        known_multis = domain_multiparts.get(domain, {})
        if not known_multis:
            continue

        for local, canon in entries:
            if canon in new_merges:
                continue
            # Only try single-token locals that are long enough to split
            parts = re.split(r'[._\-]', local)
            parts = [p for p in parts if len(p) > 1]
            if len(parts) != 1 or len(local) < 6:
                continue

            # Try all split points
            matches = []
            for split_pos in range(2, len(local) - 1):
                left = local[:split_pos]
                right = local[split_pos:]
                if len(left) < 2 or len(right) < 2:
                    continue
                key = tuple(sorted([left, right]))
                if key in known_multis:
                    target = known_multis[key]
                    if target != canon:
                        matches.append(target)

            # Only merge if exactly one match found
            unique_matches = list(set(matches))
            if len(unique_matches) == 1:
                new_merges[canon] = unique_matches[0]

    return new_merges


# ---------------------------------------------------------------------------
# Merging
# ---------------------------------------------------------------------------

def choose_canonical_node(nodes):
    """Choose the best canonical node from a group of duplicates.

    Priority: highest total count -> clean domain -> has dot separator ->
    better display name -> shorter email
    """
    def score(node):
        nid = node["id"]
        name = node.get("name", "")
        domain = node.get("domain", "")
        count = node.get("count", 0)

        # Prefer clean domains
        domain_clean = 1 if domain in ('epa.gov', 'gmail.com', 'yahoo.com') or (
            domain.endswith('.gov') and not any(c in domain for c in ('q', '3', '0'))
        ) else 0

        # Prefer dot separator in local part
        local = nid.split('@')[0] if '@' in nid else nid
        has_dot = 1 if '.' in local else 0

        # Prefer names with 2+ words, title case, no obvious OCR artifacts
        name_score = 0
        if name:
            words = name.split()
            if len(words) >= 2:
                name_score += 2
            if name == name.title() or name == name.upper():
                name_score += 1
            # Penalize names with OCR artifacts
            if any(c in name.lower() for c in ('rn', 'ii', '0', '1', '3')):
                name_score -= 1

        return (count, domain_clean, has_dot, name_score, -len(nid))

    return max(nodes, key=score)


def best_display_name(nodes):
    """Pick the best display name from a group of nodes.

    Prefers names that appear most frequently (weighted by node count),
    then by quality (2+ words, title case, no OCR artifacts).
    """
    if not nodes:
        return ""

    # Collect names weighted by node count
    name_counts = defaultdict(int)
    for n in nodes:
        name = n.get("name", "")
        if name:
            name_counts[name] += n.get("count", 1)

    if not name_counts:
        return ""

    def name_quality(name):
        freq = name_counts[name]
        words = name.split()
        has_two_words = len(words) >= 2
        is_title = name == name.title()
        # Penalize OCR artifacts (multi-char patterns only to avoid false hits)
        ocr_score = 0
        name_lower = name.lower()
        for pat in ('rn', 'ii', 'vv', 'ffl', 'svd', 'liav'):
            if pat in name_lower:
                ocr_score -= 1
        return (has_two_words, is_title, freq, ocr_score)

    return max(name_counts.keys(), key=name_quality)


# Domains known to use lastname.firstname@ format
_LASTNAME_FIRST_DOMAINS = {
    'epa.gov', 'doi.gov', 'blm.gov', 'fws.gov', 'usda.gov', 'boem.gov',
    'bsee.gov', 'osmre.gov', 'bia.gov', 'usbr.gov', 'nps.gov', 'usgs.gov',
    'state.gov',
}


def _fix_name_order(name, email_id, domain):
    """Fix name ordering for lastname.firstname@ email domains.

    For domains like epa.gov where emails are lastname.firstname@domain,
    flip the display name from 'Lastname Firstname' to 'Firstname Lastname'.
    """
    if not name or '@' not in email_id:
        return name

    # Only apply to known lastname-first domains
    if domain not in _LASTNAME_FIRST_DOMAINS:
        return name

    words = name.split()
    if len(words) != 2:
        return name

    local = email_id.split('@')[0]
    parts = re.split(r'[._\-]', local)
    parts = [p for p in parts if len(p) > 1]

    if len(parts) != 2:
        return name

    # Email is lastname.firstname@domain
    email_last, email_first = parts[0], parts[1]
    name_w0, name_w1 = words[0].lower(), words[1].lower()

    # If name matches email part order (Last First), flip to First Last
    if name_w0 == email_last and name_w1 == email_first:
        return f"{words[1]} {words[0]}"

    # Name already in First Last order or doesn't match email parts
    return name


def merge_nodes(best_id_groups, nodes_by_id):
    """Merge groups of duplicate node IDs into single nodes.

    best_id_groups: dict mapping best_original_id -> set of all original IDs in group
    """
    merged_nodes = []
    for best_id, original_ids in best_id_groups.items():
        group_nodes = [nodes_by_id[oid] for oid in original_ids if oid in nodes_by_id]
        if not group_nodes:
            continue

        best_node = nodes_by_id.get(best_id, group_nodes[0])
        name = best_display_name(group_nodes)

        # Sum counts, union years
        total_sent = sum(n.get("sent", 0) for n in group_nodes)
        total_received = sum(n.get("received", 0) for n in group_nodes)
        total_count = sum(n.get("count", 0) for n in group_nodes)
        all_years = set()
        for n in group_nodes:
            all_years.update(n.get("years", []))
        max_domain_count = max((n.get("domain_count", 0) for n in group_nodes), default=0)

        domain = normalize_domain(best_node.get("domain", ""))
        final_name = name or best_node.get("name", "")
        final_name = _fix_name_order(final_name, best_id, domain)

        merged = {
            "id": best_id,
            "name": final_name,
            "domain": domain,
            "sent": total_sent,
            "received": total_received,
            "count": total_count,
            "years": sorted(all_years),
            "domain_count": max_domain_count,
        }
        merged_nodes.append(merged)

    return merged_nodes


def merge_edges(edges, alias_map):
    """Remap edge endpoints through alias map and merge duplicate edges."""
    edge_agg = {}
    for edge in edges:
        src = alias_map.get(edge["source"], edge["source"])
        tgt = alias_map.get(edge["target"], edge["target"])

        # Skip self-loops created by merging
        if src == tgt:
            continue

        # Normalize edge key (unidirectional - preserve source->target direction)
        key = (src, tgt)

        if key in edge_agg:
            edge_agg[key]["weight"] += edge.get("weight", 1)
            edge_agg[key]["years"].update(edge.get("years", []))
        else:
            edge_agg[key] = {
                "source": src,
                "target": tgt,
                "weight": edge.get("weight", 1),
                "years": set(edge.get("years", [])),
            }

    # Convert years back to sorted lists
    merged_edges = []
    for e in edge_agg.values():
        e["years"] = sorted(e["years"])
        merged_edges.append(e)

    return merged_edges


def recompute_stats(nodes, edges):
    """Recompute top-level stats from merged data."""
    domain_counts = defaultdict(int)
    for node in nodes:
        d = node.get("domain", "")
        if d:
            domain_counts[d] += 1

    top_domains = sorted(domain_counts.items(), key=lambda x: -x[1])[:50]

    return {
        "nodes": len(nodes),
        "edges": len(edges),
        "top_domains": [{"domain": d, "count": c} for d, c in top_domains],
    }


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def _apply_layer_merges(alias_map, merges):
    """Apply a dict of canonical->canonical merges to alias_map. Returns change count."""
    changes = 0
    if not merges:
        return changes
    for src_canon, dst_canon in merges.items():
        for orig_id in list(alias_map.keys()):
            if alias_map[orig_id] == src_canon:
                alias_map[orig_id] = dst_canon
                changes += 1
    return changes


def build_alias_map(nodes, no_fuzzy=False, report=False):
    """Build complete alias map through all dedup layers.

    Returns:
        final_remap: dict mapping original node ID -> best original ID
        best_id_groups: dict mapping best original ID -> set of all original IDs
    """
    nodes_by_id = {n["id"]: n for n in nodes}
    all_original_ids = set(nodes_by_id.keys())

    # alias_map: original_id -> normalized canonical (for grouping only)
    alias_map = {nid: nid for nid in all_original_ids}

    layer_stats = []

    # --- Layer 1: Structural Cleanup ---
    changes = 0
    for nid in list(alias_map.keys()):
        cleaned = structural_cleanup(nid)
        if cleaned != nid:
            alias_map[nid] = cleaned
            changes += 1
    layer_stats.append(("Layer 1: Structural Cleanup", changes))

    # --- Layer 2: Domain Normalization ---
    changes = 0
    for nid in list(alias_map.keys()):
        current = alias_map[nid]
        normalized = apply_domain_normalization(current)
        if normalized != current:
            alias_map[nid] = normalized
            changes += 1
    layer_stats.append(("Layer 2: Domain Normalization", changes))

    # --- Layer 3: Local-Part OCR Normalization ---
    changes = 0
    for nid in list(alias_map.keys()):
        current = alias_map[nid]
        ocr_fixed = apply_local_ocr_normalization(current)
        if ocr_fixed != current:
            alias_map[nid] = ocr_fixed
            changes += 1
    layer_stats.append(("Layer 3: Local-Part OCR Normalization", changes))

    # --- Layer 4: Fuzzy Edit-Distance Matching ---
    fuzzy_merges = fuzzy_match_groups(nodes_by_id, alias_map, skip=no_fuzzy)
    changes = _apply_layer_merges(alias_map, fuzzy_merges)
    layer_stats.append(("Layer 4: Fuzzy Edit-Distance", changes))

    # --- Layer 5: Single-Part to Full-Name Matching ---
    single_merges = single_to_full_name_matches(alias_map, nodes_by_id)
    changes = _apply_layer_merges(alias_map, single_merges)
    layer_stats.append(("Layer 5: Single-Part to Full-Name", changes))

    # --- Layer 6: Concatenation Matching ---
    concat_merges = concatenation_matches(alias_map, nodes_by_id)
    changes = _apply_layer_merges(alias_map, concat_merges)
    layer_stats.append(("Layer 6: Concatenation Matching", changes))

    # Print layer stats
    print("\n=== Deduplication Layer Stats ===")
    for name, count in layer_stats:
        print(f"  {name}: {count} changes")

    # Build canonical groups (normalized_key -> set of original IDs)
    canonical_groups = defaultdict(set)
    for orig_id, canon in alias_map.items():
        canonical_groups[canon].add(orig_id)

    # For each group, pick the best original ID as the representative
    best_id_groups = {}   # best_original_id -> set of all original IDs
    final_remap = {}      # original_id -> best_original_id

    for canon_key, original_ids in canonical_groups.items():
        group_nodes = [nodes_by_id[oid] for oid in original_ids if oid in nodes_by_id]
        if not group_nodes:
            continue
        best_node = choose_canonical_node(group_nodes)
        # Clean the best ID (structural + domain normalization) but keep the
        # original local-part spelling (no OCR normalization on the output ID)
        best_id = structural_cleanup(best_node["id"])
        best_id = apply_domain_normalization(best_id)
        best_id_groups[best_id] = original_ids
        for oid in original_ids:
            final_remap[oid] = best_id

    merge_count = sum(1 for g in best_id_groups.values() if len(g) > 1)
    total_merged = sum(len(g) for g in best_id_groups.values() if len(g) > 1)
    print(f"\n  Merge groups: {merge_count}")
    print(f"  Total nodes merged: {total_merged}")
    print(f"  Unique nodes after dedup: {len(best_id_groups)}")

    if report:
        print("\n=== Merge Report (groups with 2+ members) ===")
        sorted_groups = sorted(
            [(bid, members) for bid, members in best_id_groups.items() if len(members) > 1],
            key=lambda x: -len(x[1])
        )
        for best_id, members in sorted_groups[:100]:
            print(f"\n  Best ID: {best_id}")
            for m in sorted(members):
                node = nodes_by_id.get(m)
                name = node.get("name", "") if node else ""
                count = node.get("count", 0) if node else 0
                marker = " <-- canonical" if m == best_id else ""
                print(f"    {m} ({name}, count={count}){marker}")
        if len(sorted_groups) > 100:
            print(f"\n  ... and {len(sorted_groups) - 100} more groups")

    return final_remap, best_id_groups


def run_dedup(input_path, output_path=None, dry_run=False, report=False, no_fuzzy=False):
    """Main deduplication pipeline."""
    print(f"Loading {input_path}...")
    with open(input_path, 'r') as f:
        data = json.load(f)

    nodes = data["nodes"]
    edges = data["edges"]
    orig_stats = data.get("stats", {})

    print(f"Original: {len(nodes)} nodes, {len(edges)} edges")

    orig_total_count = sum(n.get("count", 0) for n in nodes)
    print(f"Total count (sum of all node counts): {orig_total_count}")

    # Build alias map (original -> best original ID)
    final_remap, best_id_groups = build_alias_map(
        nodes, no_fuzzy=no_fuzzy, report=report
    )

    if dry_run:
        print("\n[DRY RUN] No files written.")
        return

    # Merge nodes
    nodes_by_id = {n["id"]: n for n in nodes}
    merged_nodes = merge_nodes(best_id_groups, nodes_by_id)

    # Merge edges (remap original IDs -> best original IDs)
    merged_edges = merge_edges(edges, final_remap)

    # Recompute stats
    new_stats = recompute_stats(merged_nodes, merged_edges)

    # --- Invariant checks ---
    new_total_count = sum(n.get("count", 0) for n in merged_nodes)
    print(f"\n=== Invariant Checks ===")

    # Check total count conservation
    if new_total_count != orig_total_count:
        print(f"  WARNING: Total count changed! {orig_total_count} -> {new_total_count} "
              f"(diff: {new_total_count - orig_total_count})")
    else:
        print(f"  Total count conserved: {new_total_count}")

    # Check all edge endpoints exist
    node_ids = {n["id"] for n in merged_nodes}
    bad_endpoints = 0
    for e in merged_edges:
        if e["source"] not in node_ids:
            bad_endpoints += 1
        if e["target"] not in node_ids:
            bad_endpoints += 1
    if bad_endpoints:
        print(f"  WARNING: {bad_endpoints} edge endpoints reference non-existent nodes")
    else:
        print(f"  All edge endpoints valid")

    # Check no self-loops
    self_loops = sum(1 for e in merged_edges if e["source"] == e["target"])
    if self_loops:
        print(f"  WARNING: {self_loops} self-loops found")
    else:
        print(f"  No self-loops")

    # Check no duplicate IDs
    if len(node_ids) != len(merged_nodes):
        print(f"  WARNING: Duplicate node IDs found!")
    else:
        print(f"  No duplicate node IDs")

    # Build output
    output_data = {
        "stats": new_stats,
        "nodes": merged_nodes,
        "edges": merged_edges,
    }

    # Determine output path
    if output_path is None:
        output_path = input_path

    # Backup original if overwriting
    if output_path == input_path:
        backup_path = input_path + ".bak"
        print(f"\nBacking up to {backup_path}...")
        shutil.copy2(input_path, backup_path)

    # Write output
    print(f"Writing {output_path}...")
    with open(output_path, 'w') as f:
        json.dump(output_data, f, separators=(',', ':'))

    print(f"\nDone! {len(merged_nodes)} nodes, {len(merged_edges)} edges")
    print(f"Reduction: {len(nodes) - len(merged_nodes)} nodes removed "
          f"({(len(nodes) - len(merged_nodes)) / len(nodes) * 100:.1f}%)")
    print(f"           {len(edges) - len(merged_edges)} edges removed "
          f"({(len(edges) - len(merged_edges)) / len(edges) * 100:.1f}%)")


def main():
    parser = argparse.ArgumentParser(
        description="Deduplicate email network nodes in email_network.json"
    )
    parser.add_argument(
        "input", nargs="?", default=DEFAULT_INPUT,
        help=f"Input JSON file (default: {DEFAULT_INPUT})"
    )
    parser.add_argument(
        "--output", "-o", default=None,
        help="Output JSON file (default: overwrite input)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print stats only, do not write"
    )
    parser.add_argument(
        "--report", action="store_true",
        help="Print merge groups (use with --dry-run)"
    )
    parser.add_argument(
        "--no-fuzzy", action="store_true",
        help="Skip Layer 4 (fuzzy edit-distance matching)"
    )

    args = parser.parse_args()

    if not os.path.exists(args.input):
        print(f"Error: {args.input} not found", file=sys.stderr)
        sys.exit(1)

    run_dedup(
        input_path=args.input,
        output_path=args.output,
        dry_run=args.dry_run,
        report=args.report,
        no_fuzzy=args.no_fuzzy,
    )


if __name__ == "__main__":
    main()
