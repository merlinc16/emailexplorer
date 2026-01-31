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
from itertools import permutations

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
    # cpa.* variants (c is OCR error for e)
    'cpa.gqy', 'cpa.go', 'cpa.goy', 'cpa.goyl', 'cpa.ggy', 'cpa.gm',
    'cpa.gg', 'cpa.g.qy', 'cpa.gcn', 'cpa.qov', 'cpa.aov',
    'cp3.govl', 'cp3.goy', 'cp3.qov',
    # Other prefixed/garbled EPA
    '.epa.gov', '.epa.gqy', '.epa.go', '.epa.aov',
    'ilepa.gov', 'ljcpa.gov', 'qa.gov',
    'epama.il',
}

# Full email address fixes (applied after structural cleanup)
EMAIL_FIXES = {
    'zumwalt@americanchemistry.com': 'bryan_zumwalt@americanchemistry.com',
    'bryan.ziimwalt@americanchemistry.com': 'bryan_zumwalt@americanchemistry.com',
}

# Other specific domain fixes
DOMAIN_FIXES = {
    'b1m.gov': 'blm.gov',
    'qmail.com': 'gmail.com',
    'gmial.com': 'gmail.com',
    'grnail.com': 'gmail.com',
    'grnall.com': 'gmail.com',
    'qrnail.com': 'gmail.com',
    'gmai1.com': 'gmail.com',
    # Garbled org domains
    'acvpl.org': 'acypl.org',
    'acypi.org': 'acypl.org',
    'c3i.org': 'cei.org',
    'afan.dpa.org': 'afandpa.org',
    'af3ndpa.org': 'afandpa.org',
    # DOI sub-domain garbles (preserve ios.doi.gov subdomain)
    'iosidoi.gov': 'ios.doi.gov',
    'ios.doigov': 'ios.doi.gov',
    'iosidoi.goy': 'ios.doi.gov',
    'iosdoi.gov': 'ios.doi.gov',
    'jos.doi.gov': 'ios.doi.gov',
    'os.doi.gov': 'ios.doi.gov',
    'io.s.doi.gov': 'ios.doi.gov',
    'iios.doi.gov': 'ios.doi.gov',
    'soldoi.gov': 'sol.doi.gov',
    'lsol.doi.gov': 'sol.doi.gov',
    # State domains
    'aiaska.gov': 'alaska.gov',
    # Military domain OCR garble (l -> i)
    'maii.mil': 'mail.mil',
    # ChevronTexaco renamed back to Chevron in 2005
    'chevrontexaco.com': 'chevron.com',
    'cheyron.com': 'chevron.com',
    # OCR v->y garbles
    'westgoy.org': 'westgov.org',
    'ourpublicseryice.org': 'ourpublicservice.org',
    'conseryatiye.org': 'conservative.org',
    'conseryationfund.org': 'conservationfund.org',
    'conseryamerica.org': 'conservamerica.org',
    'yenable.com': 'venable.com',
    'yerizon.net': 'verizon.net',
    'yolyo.com': 'volvo.com',
    'yalero.com': 'valero.com',
    'yocgen.com': 'vocgen.com',
    'yictoryenterprises.com': 'victoryenterprises.com',
    'yisitokc.com': 'visitokc.com',
    'liyingstongroupdc.com': 'livingstongroupdc.com',
    'liyingstongroupdc.co': 'livingstongroupdc.com',
    'hoganloyells.com': 'hoganlovells.com',
    'hoganloyeiis.com': 'hoganlovells.com',
    'hoganjoyells.com': 'hoganlovells.com',
    'nayigatorsglobal.com': 'navigatorsglobal.com',
    'gayelresources.com': 'gavelresources.com',
    'coloradoliyestock.org': 'coloradolivestock.org',
    'colostate.edu': 'colostate.edu',
    'hoydengrayassociates.com': 'boydengrayassociates.com',
    'hhqyentures.com': 'hhqventures.com',
    'hewelleyents.com': 'hewellevents.com',
    'toxseryices.com': 'toxservices.com',
    'public.goydeliyery.com': 'public.govdelivery.com',
    'seryice.goydeliyery.com': 'service.govdelivery.com',
    'bcdtrayel.com': 'bcdtravel.com',
    'creatiye-mill.com': 'creative-mill.com',
    'inyariantgr.com': 'invariantgr.com',
    # OCR rn->m garbles
    'dailycallemewsfoundation.org': 'dailycallernewsfoundation.org',
    'bockomygroup.com': 'bockornygroup.com',
    'hockomygroup.com': 'bockornygroup.com',
    'bqckomygrqup.com': 'bockornygroup.com',
    'bockomygroup.co': 'bockornygroup.com',
    'bockomygrotip.com': 'bockornygroup.com',
    'southemco.com': 'southernco.com',
    # OCR 1->l, i->l garbles
    'sidiey.com': 'sidley.com',
    'sidiey.co': 'sidley.com',
    'hollandliart.com': 'hollandhart.com',
    'honandhart.com': 'hollandhart.com',
    'hqllandhart.com': 'hollandhart.com',
    'nelsonmiillins.com': 'nelsonmullins.com',
    'nelsonmullms.com': 'nelsonmullins.com',
    'nelsonmiillins.com': 'nelsonmullins.com',
    'aiuminum.org': 'aluminum.org',
    'aiphq.org': 'afphq.org',
    'afpni.org': 'afphq.org',
    'cargili.com': 'cargill.com',
    'cargin.com': 'cargill.com',
    'conocophiliips.com': 'conocophillips.com',
    'conocophijlips.com': 'conocophillips.com',
    'conocophiglips.com': 'conocophillips.com',
    'conocophihips.com': 'conocophillips.com',
    'conocophiljips.com': 'conocophillips.com',
    'conocoohiilids.co': 'conocophillips.com',
    'conocophiilips.com': 'conocophillips.com',
    'conocophiyips.co': 'conocophillips.com',
    'bqeing.com': 'boeing.com',
    'archcoai.com': 'archcoal.com',
    'consoleiiergy.com': 'consolenergy.com',
    'gmaii.com': 'gmail.com',
    'listserye.api.org': 'listserv.api.org',
    'alphagrpdc.com': 'alphagrpdc.com',
    'aiphagrpdc.com': 'alphagrpdc.com',
    # OCR 3->a garbles
    'herifage.org': 'heritage.org',
    'hcritage.org': 'heritage.org',
    'hentage.org': 'heritage.org',
    'americanchemisfry.com': 'americanchemistry.com',
    'americanchcmisry.com': 'americanchemistry.com',
    'americancheniistry.com': 'americanchemistry.com',
    'amerieanchemistry.com': 'americanchemistry.com',
    'americanchemistry.coni': 'americanchemistry.com',
    'americanchemistfy.co': 'americanchemistry.com',
    'amerlearichemistry.com': 'americanchemistry.com',
    'americaiichemistry.com': 'americanchemistry.com',
    # Various OCR garbles of croplifeamerica.org
    'crqplifeamerica.org': 'croplifeamerica.org',
    'cropnfeamerica.org': 'croplifeamerica.org',
    'cropiifeamerica.org': 'croplifeamerica.org',
    'croplifeameriea.org': 'croplifeamerica.org',
    'croplifeamenca.org': 'croplifeamerica.org',
    'cropisfeaniersca.org': 'croplifeamerica.org',
    'crqpiifearoeriea.org': 'croplifeamerica.org',
    'crophfeamerica.org': 'croplifeamerica.org',
    'croplifearoerica.org': 'croplifeamerica.org',
    'cfopiifeamefica.org': 'croplifeamerica.org',
    'cfqplifeamerica.org': 'croplifeamerica.org',
    'cropkfeamerica.oig': 'croplifeamerica.org',
    'cropgsfeamerica.org': 'croplifeamerica.org',
    'crqpsifeamenea.org': 'croplifeamerica.org',
    'crqpjifeameriea.org': 'croplifeamerica.org',
    'cropisfeamersca.org': 'croplifeamerica.org',
    'cropsifeaniefica.org': 'croplifeamerica.org',
    'cropnfeanierica.orr': 'croplifeamerica.org',
    'cropiifeamenca.org': 'croplifeamerica.org',
    # Other suffix garbles
    'ge.co': 'ge.com',
    'cbsnews.co': 'cbsnews.com',
    'socma.co': 'socma.com',
    'nahb.ofg': 'nahb.org',
    'nahb.grg': 'nahb.org',
    'lung.ofg': 'lung.org',
    'nam.ofg': 'nam.org',
    'okfb.ofg': 'okfb.org',
    'awwa.ofg': 'awwa.org',
    'qkfb.org': 'okfb.org',
    'growtheneray.org': 'growthenergy.org',
    'nohle.org': 'noble.org',
    'sallt.com': 'salt.com',
    'loyes.com': 'loves.com',
    'miningamerica.org': 'miningamerica.org',
    # Various other OCR garbles
    'dowcoming.com': 'dowcorning.com',
    'chsnews.com': 'cbsnews.com',
    'hsph.haryard.edu': 'hsph.harvard.edu',
    'wms-jen.com': 'wms-jen.com',
    'lawa6o.com': 'lawa60.com',
    '72ostrategies.com': '720strategies.com',
    'gps-5o.com': 'gps-50.com',
    'cfaeorp.com': 'cfacorp.com',
    'painf.org': 'paint.org',
    'eaest.com': 'east.com',
    'dorox.com': 'dorox.com',
    'dqw.com': 'dow.com',
}

# OCR character substitutions for domains
DOMAIN_OCR_CHAR_MAP = {
    'rn': 'm',
    '1': 'l',
    '3': 'a',
    '0': 'o',
    'v': 'y',
}

# OCR character substitutions for local parts
LOCAL_OCR_CHAR_MAP = {
    'ffl': 'm',   # OCR ligature for 'm'
    'ffi': 'n',   # OCR ligature for 'n'
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
    r'[rfnc]?mailto[i1l:;c]\s*'       # mailto: with optional stray prefix (c = garbled colon)
    r'|[rfnc]?rnailto[i1l:;c]\s*'     # rn→m OCR variant
    r'|[rfnc]?rnai[il1]to[i1l:;c]\s*' # double OCR variant
    r'|[rfnc]?mai[il1]to[i1l:;c]\s*'  # mail OCR variant
    r'|mail\.to[i1l:;c]\s*'           # mail.to: (dot-separated)
    r'|[rfnc]?mailtcr\s*'             # mailtcr (garbled mailto)
    r'|[rfnc]?mai[il1]sto[i1l:;c]\s*' # mailsto (extra s)
    r'|[1l]to[i1l:;c]\s*'             # partial mailto suffix (lto:) from line break
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

    # Replace hyphens with dots in local part (OCR misreads dots as hyphens)
    local = local.replace('-', '.')

    # Collapse double dots
    while '..' in local:
        local = local.replace('..', '.')
    while '..' in domain:
        domain = domain.replace('..', '.')

    # Strip leading/trailing dots and hyphens from domain
    domain = domain.strip('.-')

    result = f"{local}@{domain}" if local and domain else email

    # Apply known full-email fixes
    if result in EMAIL_FIXES:
        result = EMAIL_FIXES[result]

    return result


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

    # State EPA domains - preserve (but not 'ilepa.gov' which is OCR garble)
    if domain == 'iepa.gov':
        return domain
    if domain == 'calepa.ca.gov':
        return domain

    # Specific domain fixes
    if domain in DOMAIN_FIXES:
        return DOMAIN_FIXES[domain]

    # Collapse dots within TLD components (e.g., cpa.g.qy -> cpa.gqy)
    # OCR sometimes inserts dots within the TLD
    parts = domain.split('.')
    if len(parts) >= 3:
        # Try joining the last 2 parts if they're both short (likely a split TLD)
        last_two = parts[-2] + parts[-1]
        if len(parts[-2]) <= 2 and len(parts[-1]) <= 3 and len(last_two) <= 4:
            domain = '.'.join(parts[:-2]) + '.' + last_two
        # Also try joining last 3 parts if all very short
        elif len(parts) >= 4 and all(len(p) <= 2 for p in parts[-3:]):
            joined = ''.join(parts[-3:])
            if len(joined) <= 5:
                domain = '.'.join(parts[:-3]) + '.' + joined

    # Re-check EPA errors after dot collapse
    if domain in EPA_ERROR_DOMAINS:
        return 'epa.gov'

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
            ('.gqy', '.gov'), ('.ggy', '.gov'),  # OCR garble of .gov
            ('.gg', '.gov'),   # truncated + garbled
            ('.eom', '.com'), ('.corn', '.com'), ('.coml', '.com'),
            ('.comi', '.com'),
            ('.orq', '.org'), ('.orql', '.org'),
            ('.ora', '.org'), ('.ore', '.org'),  # OCR garble of .org
            ('.orgl', '.org'),
            ('.edul', '.edu'),
        ]:
            if domain.endswith(bad_suffix):
                domain = domain[:-len(bad_suffix)] + good_suffix
                changed = True
                break
        # Handle truncated .gov -> .go (only for hosts that look governmental)
        if not changed and domain.endswith('.go') and not domain.endswith('.go.'):
            host = domain[:-3]
            if host and len(host.split('.')[-1]) <= 5:
                domain = domain + 'v'  # .go -> .gov
                changed = True
        # Strip trailing l/1/j/i from TLDs (e.g., .goyl -> .goy -> .gov)
        if not changed and len(domain) > 4:
            tld = domain.rsplit('.', 1)[-1]
            if tld.endswith(('l', '1', 'j')) and tld not in ('html', 'mil'):
                domain = domain[:-1]
                changed = True
        if not changed:
            break

    # Re-check EPA errors and DOMAIN_FIXES after suffix normalization
    if domain in EPA_ERROR_DOMAINS:
        return 'epa.gov'
    if domain in DOMAIN_FIXES:
        return DOMAIN_FIXES[domain]

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
    if domain in DOMAIN_FIXES:
        return DOMAIN_FIXES[domain]

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
    """Apply OCR character substitutions to local part (for dedup matching only)."""
    result = local
    # Apply substitutions in order from longest pattern to shortest
    # to avoid partial matches (e.g., 'rn' before single-char subs)
    for ocr_err, fix in sorted(LOCAL_OCR_CHAR_MAP.items(),
                                key=lambda x: -len(x[0])):
        result = result.replace(ocr_err, fix)
    return result


def ocr_clean_local_for_display(local):
    """Clean OCR errors in the local part for display purposes.

    Conservative: only fixes digits clearly embedded in alphabetic name parts.
    Does NOT apply rn->m, ii->n, v->y or other letter-to-letter substitutions
    because those have too many false positives (e.g. bernhardt, tierney, barnes).
    """
    # Split into name parts (by . or _)
    parts = re.split(r'([._])', local)
    cleaned = []
    for part in parts:
        if part in ('.', '_'):
            cleaned.append(part)
            continue
        # If part is purely digits, keep as-is
        if re.match(r'^\d+$', part):
            cleaned.append(part)
            continue
        # Fix digits embedded in alpha parts (surrounded by letters)
        # Common OCR digit-to-letter confusions:
        result = re.sub(r'(?<=[a-z])1(?=[a-z])', 'l', part)   # 1 -> l
        result = re.sub(r'(?<=[a-z])0(?=[a-z])', 'o', result)  # 0 -> o
        result = re.sub(r'(?<=[a-z])3(?=[a-z])', 'e', result)  # 3 -> e
        result = re.sub(r'(?<=[a-z])8(?=[a-z])', 'b', result)  # 8 -> b
        result = re.sub(r'(?<=[a-z])5(?=[a-z])', 's', result)  # 5 -> s
        result = re.sub(r'(?<=[a-z])6(?=[a-z])', 'b', result)  # 6 -> b
        result = re.sub(r'(?<=[a-z])2(?=[a-z])', 'z', result)  # 2 -> z
        # Fix leading digit before 3+ letters
        result = re.sub(r'^3(?=[a-z]{3,})', 'e', result)
        result = re.sub(r'^1(?=[a-z]{3,})', 'l', result)
        result = re.sub(r'^0(?=[a-z]{3,})', 'o', result)
        result = re.sub(r'^6(?=[a-z]{3,})', 'b', result)
        result = re.sub(r'^5(?=[a-z]{3,})', 's', result)
        cleaned.append(result)
    return ''.join(cleaned)


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
# Layer 3b: Join Split Local Parts
# ---------------------------------------------------------------------------

def join_split_local_matches(alias_map, all_original_ids):
    """For emails with 3+ local parts, try joining parts to match existing
    2-part canonicals. OCR sometimes inserts dots in the middle of names
    (e.g., 'syd.ney' -> 'sydney', 'svdn.ev' -> 'svdnev' -> 'sydney').

    Works on original (unsorted) part order since joining depends on
    which parts were adjacent in the original text.
    """
    # Build set of 2-part canonicals per domain: sorted_tuple -> canonical key
    two_part = defaultdict(dict)
    for canon in set(alias_map.values()):
        if '@' not in canon:
            continue
        local, domain = canon.split('@', 1)
        parts = re.split(r'[._\-]', local)
        parts = [p for p in parts if len(p) > 1]
        if len(parts) == 2:
            two_part[domain][tuple(sorted(parts))] = canon

    new_merges = {}  # canonical -> target canonical

    # Process each original ID to access unsorted part order
    seen_canons = set()
    for orig_id in all_original_ids:
        canon = alias_map[orig_id]
        if canon in new_merges or canon in seen_canons:
            continue
        seen_canons.add(canon)
        if '@' not in canon:
            continue
        _, domain = canon.split('@', 1)

        known = two_part.get(domain)
        if not known:
            continue

        # Get unsorted parts from the structural+domain cleaned original
        cleaned = structural_cleanup(orig_id)
        cleaned = apply_domain_normalization(cleaned)
        if '@' not in cleaned:
            continue
        orig_local = cleaned.split('@')[0]
        orig_parts = re.split(r'[._\-]', orig_local)
        orig_parts = [p for p in orig_parts if len(p) > 1]
        if len(orig_parts) < 3:
            continue

        # Try all ways to join parts into 2 groups.
        # For 3 parts [a,b,c], try all pair joins in both orders:
        #   (ab,c), (ba,c), (ac,b), (ca,b), (bc,a), (cb,a)
        # For 4+ parts, try consecutive splits plus pair combinations
        best_match = None

        if len(orig_parts) == 3:
            a, b, c = orig_parts
            join_candidates = [
                (a + b, c), (b + a, c),
                (a + c, b), (c + a, b),
                (b + c, a), (c + b, a),
            ]
        else:
            # For 4+ parts, try all consecutive splits
            join_candidates = []
            for split in range(1, len(orig_parts)):
                left = ''.join(orig_parts[:split])
                right = ''.join(orig_parts[split:])
                join_candidates.append((left, right))

        for left, right in join_candidates:
            if len(left) < 2 or len(right) < 2:
                continue
            # Try with OCR normalization
            left_n = ocr_normalize_local(left)
            right_n = ocr_normalize_local(right)
            key = tuple(sorted([left_n, right_n]))
            if key in known:
                target = known[key]
                if target != canon:
                    best_match = target
                    break
            # Try without OCR
            key2 = tuple(sorted([left, right]))
            if key2 in known:
                target = known[key2]
                if target != canon:
                    best_match = target
                    break

        if best_match:
            new_merges[canon] = best_match

    return new_merges


# ---------------------------------------------------------------------------
# Layer 3c: Prefix Stripping (concatenated garbage)
# ---------------------------------------------------------------------------

def prefix_strip_matches(alias_map):
    """Detect and strip concatenated garbage from local parts.

    OCR sometimes concatenates content from a previous field onto the email,
    e.g., 'sydneyfhupp.sydney@epa.gov' where 'sydneyf' is garbage from
    a previous email field.

    Checks ALL parts for known name fragments at both start and end.
    """
    # Collect known name parts per domain (from 2-part canonicals)
    domain_name_parts = defaultdict(set)
    two_part_canonicals = defaultdict(dict)
    for canon in set(alias_map.values()):
        if '@' not in canon:
            continue
        local, domain = canon.split('@', 1)
        parts = re.split(r'[._\-]', local)
        parts = [p for p in parts if len(p) > 1]
        if len(parts) == 2:
            for p in parts:
                domain_name_parts[domain].add(p)
            two_part_canonicals[domain][tuple(sorted(parts))] = canon

    new_merges = {}

    for canon in set(alias_map.values()):
        if canon in new_merges:
            continue
        if '@' not in canon:
            continue
        local, domain = canon.split('@', 1)
        known_parts = domain_name_parts.get(domain)
        if not known_parts:
            continue
        known_twopart = two_part_canonicals.get(domain, {})

        parts = re.split(r'[._\-]', local)
        parts = [p for p in parts if p]
        if len(parts) < 2:
            continue

        found = False
        for i, part in enumerate(parts):
            if found:
                break
            # Check if this part ENDS with a known name (garbage prefix + name)
            for known_p in known_parts:
                if len(known_p) < 3:
                    continue
                if part.endswith(known_p) and len(part) > len(known_p):
                    stripped = known_p
                    remaining = parts[:i] + [stripped] + parts[i+1:]
                    remaining = [p for p in remaining if len(p) > 1]
                    if len(remaining) == 2:
                        key = tuple(sorted(remaining))
                        if key in known_twopart:
                            target = known_twopart[key]
                            if target != canon:
                                new_merges[canon] = target
                                found = True
                                break
            if found:
                break
            # Check if this part STARTS with a known name (name + garbage suffix)
            for known_p in known_parts:
                if len(known_p) < 3:
                    continue
                if part.startswith(known_p) and len(part) > len(known_p):
                    stripped = known_p
                    remaining = parts[:i] + [stripped] + parts[i+1:]
                    remaining = [p for p in remaining if len(p) > 1]
                    if len(remaining) == 2:
                        key = tuple(sorted(remaining))
                        if key in known_twopart:
                            target = known_twopart[key]
                            if target != canon:
                                new_merges[canon] = target
                                found = True
                                break

    return new_merges


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
                threshold = max(2, shorter // 5)

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
                    # Use Jaro-Winkler on full names
                    jw = jaro_winkler(ni.lower(), nj.lower())
                    # Token overlap: compare sorted word sets
                    w1 = set(ni.lower().split())
                    w2 = set(nj.lower().split())
                    common = len(w1 & w2)
                    total = len(w1 | w2)
                    token_sim = common / total if total else 1.0
                    # Local-part word overlap (split by dot)
                    li_parts = set(p for p in li.split('.') if len(p) >= 3)
                    lj_parts = set(p for p in lj.split('.') if len(p) >= 3)
                    shared_local = bool(li_parts & lj_parts)
                    if jw < 0.85 and token_sim < 0.4 and not shared_local:
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
        # Find the member with the highest count (deterministic tiebreaker: ID)
        best = max(members, key=lambda c: (_total_count_for_canonical(
            c, canonical_to_originals, nodes_by_id), c))
        for m in members:
            if m != best:
                new_merges[m] = best

    return new_merges


def _best_name_for_canonical(canonical, canonical_to_originals, nodes_by_id):
    """Get the best display name among all original nodes for a canonical.
    Uses count-weighted frequency to avoid picking garbled OCR names."""
    originals = canonical_to_originals.get(canonical, [canonical])
    name_counts = defaultdict(int)
    for oid in originals:
        node = nodes_by_id.get(oid)
        if node and node.get("name"):
            name_counts[node["name"]] += node.get("count", 1)
    if not name_counts:
        return ""
    # Pick name with highest count, preferring title case and 2+ words
    def score(name):
        freq = name_counts[name]
        words = name.split()
        has_words = len(words) >= 2
        is_title = name == name.title()
        ocr_penalty = sum(1 for p in ('rn', 'ii', 'ffl', 'ffi', '0', '1', '3')
                          if p in name.lower())
        return (has_words, is_title, -ocr_penalty, freq, name)
    return max(name_counts.keys(), key=score)


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
    'epa.gov',
}


_COMMON_FIRST_NAMES = {
    'aaron', 'adam', 'adrian', 'alan', 'albert', 'alex', 'alexander',
    'alfred', 'alice', 'alicia', 'alison', 'allen', 'allison', 'amanda',
    'amber', 'amy', 'andrea', 'andrew', 'angela', 'ann', 'anna', 'anne',
    'annie', 'anthony', 'april', 'arthur', 'ashley', 'barbara', 'barry',
    'benjamin', 'bernard', 'beth', 'bethany', 'betty', 'beverly', 'bill',
    'billy', 'blake', 'bobby', 'bonnie', 'brad', 'bradley', 'brenda',
    'brendan', 'brent', 'brett', 'brian', 'bridget', 'brittany', 'brook',
    'brooke', 'bruce', 'bryan', 'calvin', 'cameron', 'carl', 'carol',
    'caroline', 'carolyn', 'catherine', 'chad', 'charles', 'charlotte',
    'cheryl', 'chris', 'christian', 'christina', 'christine', 'christopher',
    'cindy', 'claire', 'clarence', 'clark', 'claudia', 'clifford', 'clint',
    'cody', 'cole', 'colin', 'connie', 'connor', 'corey', 'craig',
    'crystal', 'cynthia', 'dale', 'dallas', 'dana', 'daniel', 'danny',
    'darren', 'dave', 'david', 'dawn', 'dean', 'debbie', 'deborah',
    'debra', 'denise', 'dennis', 'derek', 'derrick', 'diana', 'diane',
    'don', 'donald', 'donna', 'doris', 'dorothy', 'doug', 'douglas',
    'drew', 'dustin', 'dylan', 'earl', 'eddie', 'edward', 'eileen',
    'elaine', 'elizabeth', 'ellen', 'emily', 'emma', 'eric', 'erica',
    'erin', 'ernest', 'eugene', 'eva', 'evan', 'evelyn', 'faith',
    'florence', 'frances', 'francis', 'frank', 'fred', 'frederick',
    'gabriel', 'gary', 'gavin', 'gene', 'george', 'gerald', 'gina',
    'glen', 'glenn', 'gloria', 'gordon', 'grace', 'grant', 'greg',
    'gregory', 'gwen', 'hannah', 'harold', 'harry', 'harvey', 'heather',
    'helen', 'henry', 'herbert', 'holly', 'howard', 'hunter', 'irene',
    'isaac', 'ivan', 'jack', 'jackie', 'jacob', 'jacqueline', 'james',
    'jamie', 'jane', 'janet', 'janice', 'jared', 'jasmine', 'jason',
    'jean', 'jeff', 'jeffrey', 'jennifer', 'jenny', 'jeremy', 'jerry',
    'jesse', 'jessica', 'jill', 'jimmy', 'joan', 'joanne', 'jocelyn',
    'jody', 'joel', 'john', 'johnny', 'jonathan', 'jordan', 'joseph',
    'joshua', 'joyce', 'judith', 'judy', 'julia', 'julian', 'julie',
    'justin', 'karen', 'karl', 'kate', 'katherine', 'kathleen', 'kathryn',
    'kathy', 'katie', 'keith', 'kelly', 'ken', 'kenneth', 'kevin',
    'kimberly', 'kirk', 'kristen', 'kristin', 'kristina', 'kurt', 'kyle',
    'lance', 'larry', 'laura', 'lauren', 'laurie', 'lawrence', 'leah',
    'lee', 'leon', 'leonard', 'leslie', 'lillian', 'linda', 'lindsay',
    'lisa', 'lois', 'loretta', 'lori', 'louis', 'louise', 'lucas', 'luke',
    'lynn', 'madison', 'marc', 'marcus', 'margaret', 'maria', 'marie',
    'marilyn', 'marion', 'mark', 'marsha', 'martha', 'martin', 'marvin',
    'mary', 'matt', 'matthew', 'maureen', 'max', 'megan', 'melissa',
    'michael', 'michele', 'michelle', 'mike', 'miles', 'miranda', 'misty',
    'mitchell', 'molly', 'monica', 'morgan', 'morris', 'nancy', 'natalie',
    'nathan', 'neil', 'nelson', 'nicholas', 'nicole', 'noah', 'norma',
    'norman', 'oliver', 'olivia', 'oscar', 'owen', 'paige', 'pamela',
    'patricia', 'patrick', 'paul', 'paula', 'peggy', 'penny', 'peter',
    'philip', 'phillip', 'phyllis', 'rachel', 'ralph', 'randy', 'raymond',
    'rebecca', 'regina', 'renee', 'rhonda', 'richard', 'rick', 'rita',
    'robert', 'robin', 'rodney', 'roger', 'roland', 'ronald', 'rose',
    'ross', 'roxanne', 'roy', 'ruby', 'russell', 'ruth', 'ryan',
    'sabrina', 'sally', 'samantha', 'samuel', 'sandra', 'sandy', 'sara',
    'sarah', 'scott', 'sean', 'seth', 'shane', 'shannon', 'sharon',
    'sheila', 'shelley', 'sherry', 'shirley', 'sophia', 'stacey',
    'stacy', 'stanley', 'stefanie', 'stephanie', 'stephen', 'steve',
    'steven', 'stuart', 'susan', 'suzanne', 'sydney', 'sylvia', 'tamara',
    'tammy', 'tanya', 'tara', 'taylor', 'teresa', 'terri', 'terry',
    'thelma', 'theresa', 'thomas', 'tiffany', 'timothy', 'tina', 'todd',
    'tommy', 'tony', 'tracy', 'travis', 'trevor', 'troy', 'tyler',
    'valerie', 'vanessa', 'vernon', 'veronica', 'vicki', 'victoria',
    'vincent', 'virginia', 'vivian', 'wade', 'walter', 'wanda', 'warren',
    'wayne', 'wendy', 'wesley', 'whitney', 'william', 'willie', 'yolanda',
    'zachary',
}

def _split_initial_name(name):
    """Split a concatenated initial+lastname into 'F. Lastname'.
    E.g., 'Jgreen' -> 'J. Green', 'Mthompson' -> 'M. Thompson'.
    Returns None if the name is a common first name or doesn't match pattern.
    """
    if not name or len(name) < 5:
        return None
    words = name.split()
    if len(words) != 1:
        return None
    word = words[0]
    # Don't split common first names or generic words
    if word.lower() in _COMMON_FIRST_NAMES:
        return None
    if word.lower() in _GENERIC_LOCALS:
        return None
    # Don't split common English words
    if word.lower() in {
        'press', 'scheduling', 'requests', 'records', 'planning',
        'counsel', 'director', 'manager', 'editor', 'congress',
        'intern', 'orders', 'updates', 'alerts', 'comments',
        'regulation', 'regulatory', 'operations', 'program',
        'executive', 'chairman', 'president', 'secretary',
        'treasurer', 'governor', 'senator', 'representative',
    }:
        return None
    if not word[0].isupper():
        return None
    rest = word[1:]
    if len(rest) < 3:
        return None
    return f"{word[0]}. {rest.title()}"


def _name_from_email(email_id):
    """Generate a display name from an email address local part.
    E.g., 'hupp.sydney@epa.gov' -> 'Hupp Sydney'.
    For single-part locals like 'jgreen', tries to split into 'J. Green'.
    Returns empty string if no sensible name can be derived.
    """
    if '@' not in email_id:
        return ""
    local = email_id.split('@')[0]
    parts = re.split(r'[._\-]', local)
    parts = [p for p in parts if len(p) > 1]
    if not parts:
        return ""
    # Skip if looks like an org mailbox
    if len(parts) == 1 and parts[0].lower() in _GENERIC_LOCALS:
        return ""
    if len(parts) == 1:
        # Try splitting initial + lastname
        split = _split_initial_name(parts[0].title())
        if split:
            return split
    return ' '.join(p.title() for p in parts)


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
        # Generate name from email if none exists
        if not final_name:
            final_name = _name_from_email(best_id)
        # Split concatenated initial+lastname (e.g., "Mshut" -> "M. Shut")
        split = _split_initial_name(final_name)
        if split:
            final_name = split
        final_name = _fix_name_order(final_name, best_id, domain)

        # Collect all original email aliases for MongoDB search
        all_aliases = sorted(original_ids)

        merged = {
            "id": best_id,
            "name": final_name,
            "domain": domain,
            "sent": total_sent,
            "received": total_received,
            "count": total_count,
            "years": sorted(all_years),
            "domain_count": max_domain_count,
            "aliases": all_aliases,
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
            edge_agg[key]["doc_ids"].update(edge.get("doc_ids", []))
        else:
            edge_agg[key] = {
                "source": src,
                "target": tgt,
                "weight": edge.get("weight", 1),
                "years": set(edge.get("years", [])),
                "doc_ids": set(edge.get("doc_ids", [])),
            }

    # Convert sets back to sorted lists
    merged_edges = []
    for e in edge_agg.values():
        e["years"] = sorted(e["years"])
        e["doc_ids"] = sorted(e["doc_ids"])
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

_GENERIC_LOCALS = {
    'info', 'admin', 'support', 'contact', 'office', 'mail', 'webmaster',
    'sales', 'noreply', 'help', 'service', 'news', 'media', 'press',
    'marketing', 'hr', 'legal', 'compliance', 'jobs', 'careers', 'events',
    'feedback', 'billing', 'security', 'postmaster', 'abuse', 'root',
    'team', 'hello', 'general', 'inquiries', 'membership', 'scheduling',
    'requests', 'records', 'orders', 'alerts', 'comments', 'updates',
    'planning', 'operations', 'regulation', 'program', 'intern',
    'counsel', 'director', 'chairman', 'editor', 'congress',
}

def _same_name_merge(alias_map, nodes_by_id):
    """Merge nodes with identical display names, handling cross-domain OCR garbling.

    Two strategies:
    1. Same domain: merge nodes with identical normalized names (2+ words)
    2. Cross-domain: merge nodes with identical local part AND identical name,
       where the domains are similar (fuzzy match) — handles domain OCR errors
    """
    canonical_to_originals = defaultdict(list)
    for orig_id, canon in alias_map.items():
        canonical_to_originals[canon].append(orig_id)

    new_merges = {}

    # --- Strategy 1: Same domain, same normalized name ---
    domain_name_groups = defaultdict(list)
    for canon in set(alias_map.values()):
        if '@' not in canon:
            continue
        domain = canon.split('@', 1)[1]
        name = _best_name_for_canonical(canon, canonical_to_originals, nodes_by_id)
        if not name:
            continue
        words = name.lower().split()
        if len(words) < 2:
            continue
        norm_name = ' '.join(sorted(words))
        count = _total_count_for_canonical(canon, canonical_to_originals, nodes_by_id)
        domain_name_groups[(domain, norm_name)].append((canon, count))

    for (domain, norm_name), entries in domain_name_groups.items():
        if len(entries) < 2:
            continue
        entries.sort(key=lambda x: -x[1])
        best = entries[0][0]
        for canon, count in entries[1:]:
            if canon not in new_merges:
                new_merges[canon] = best

    # --- Strategy 1b: Cross-domain, same local, similar domain ---
    # For entries with identical canonical locals on OCR-similar domains,
    # merge regardless of display name. OCR often garbles both domain AND
    # name, so name matching can't catch these.
    # Skip generic/common locals to avoid false merges.
    local_domain_groups = defaultdict(list)
    for canon in sorted(set(alias_map.values())):
        if canon in new_merges:
            continue
        if '@' not in canon:
            continue
        local, domain = canon.split('@', 1)
        local_clean = re.split(r'[._\-]', local)
        local_clean = [p for p in local_clean if p]
        # Skip generic locals and common first names (too ambiguous)
        if len(local_clean) == 1 and local_clean[0] in _GENERIC_LOCALS:
            continue
        if local.lower() in _COMMON_FIRST_NAMES:
            continue
        if len(local) <= 3:
            continue
        count = _total_count_for_canonical(canon, canonical_to_originals, nodes_by_id)
        local_domain_groups[local].append((canon, domain, count))

    uf1b = _UnionFind()
    for local, entries in local_domain_groups.items():
        if len(entries) < 2:
            continue
        for canon, domain, count in entries:
            uf1b.add(canon, count)
        for i in range(len(entries)):
            ci, di, cnti = entries[i]
            for j in range(i + 1, len(entries)):
                cj, dj, cntj = entries[j]
                if uf1b.find(ci) == uf1b.find(cj):
                    continue
                dist = levenshtein(di, dj)
                max_dlen = max(len(di), len(dj))
                threshold = max(3, max_dlen // 3)
                if dist <= threshold and dist > 0:
                    uf1b.union(ci, cj)

    for rep, members in uf1b.groups().items():
        if len(members) <= 1:
            continue
        best = max(members, key=lambda c: (_total_count_for_canonical(
            c, canonical_to_originals, nodes_by_id), c))
        for m in members:
            if m != best and m not in new_merges:
                new_merges[m] = best

    # --- Strategy 2: Cross-domain, same local + same name ---
    # Group by (local_part, normalized_name), then use pairwise Union-Find
    # so that domain-similar entries merge even if neither is the best.
    local_name_groups = defaultdict(list)
    is_generic_local = {}
    for canon in sorted(set(alias_map.values())):
        if canon in new_merges:
            continue
        if '@' not in canon:
            continue
        local, domain = canon.split('@', 1)
        local_clean = re.split(r'[._\-]', local)
        local_clean = [p for p in local_clean if p]
        is_generic = len(local_clean) == 1 and local_clean[0] in _GENERIC_LOCALS
        name = _best_name_for_canonical(canon, canonical_to_originals, nodes_by_id)
        if not name:
            continue
        norm_name = ' '.join(sorted(name.lower().split()))
        count = _total_count_for_canonical(canon, canonical_to_originals, nodes_by_id)
        local_name_groups[(local, norm_name)].append((canon, domain, count))
        is_generic_local[local] = is_generic

    uf2 = _UnionFind()

    for (local, norm_name), entries in local_name_groups.items():
        if len(entries) < 2:
            continue

        # Generic locals AND common first names require domain similarity
        require_domain_check = (
            is_generic_local.get(local, False)
            or local.lower() in _COMMON_FIRST_NAMES
            or len(local) <= 4
        )

        for canon, domain, count in entries:
            uf2.add(canon, count)

        # Pairwise comparison within each group
        for i in range(len(entries)):
            ci, di, cnti = entries[i]
            for j in range(i + 1, len(entries)):
                cj, dj, cntj = entries[j]
                if uf2.find(ci) == uf2.find(cj):
                    continue
                if require_domain_check:
                    dist = levenshtein(di, dj)
                    max_domain_len = max(len(di), len(dj))
                    threshold = max(3, max_domain_len // 3)
                    if dist > threshold:
                        continue
                uf2.union(ci, cj)

    # --- Strategy 3: Cross-domain, fuzzy local + same name ---
    # For entries with similar locals and identical names, merge cross-domain.
    # Uses both full-string and part-level fuzzy matching to handle OCR
    # errors that change alphabetical sort order of local parts.
    name_groups = defaultdict(list)
    for canon in sorted(set(alias_map.values())):
        if canon in new_merges:
            continue
        if '@' not in canon:
            continue
        local, domain = canon.split('@', 1)
        local_clean = re.split(r'[._\-]', local)
        local_clean = [p for p in local_clean if p]
        is_generic = len(local_clean) == 1 and local_clean[0] in _GENERIC_LOCALS
        name = _best_name_for_canonical(canon, canonical_to_originals, nodes_by_id)
        if not name:
            continue
        norm_name = ' '.join(sorted(name.lower().split()))
        count = _total_count_for_canonical(canon, canonical_to_originals, nodes_by_id)
        local_parts = sorted(re.split(r'[._\-]', local))
        name_groups[norm_name].append((canon, local, domain, count, local_parts, is_generic))

    for norm_name, entries in name_groups.items():
        if len(entries) < 2:
            continue
        for canon, local, domain, count, parts, is_generic in entries:
            uf2.add(canon, count)

        for i in range(len(entries)):
            ci, li, di, cnti, pi, gi = entries[i]
            for j in range(i + 1, len(entries)):
                cj, lj, dj, cntj, pj, gj = entries[j]
                if uf2.find(ci) == uf2.find(cj):
                    continue
                # Try full-string fuzzy match first
                local_dist = levenshtein(li, lj)
                shorter_local = min(len(li), len(lj))
                if shorter_local < 3:
                    continue
                local_threshold = max(2, shorter_local // 4)
                matched = local_dist <= local_threshold
                # If full-string fails, try part-level matching
                # (handles cases where OCR changes sort order of parts)
                if not matched and len(pi) == len(pj) and len(pi) >= 2:
                    # Try both orderings of parts
                    best_part_dist = float('inf')
                    for perm in permutations(range(len(pj))):
                        total = sum(levenshtein(pi[k], pj[perm[k]])
                                    for k in range(len(pi)))
                        best_part_dist = min(best_part_dist, total)
                    part_threshold = max(2, sum(len(p) for p in pi) // 4)
                    matched = best_part_dist <= part_threshold
                if not matched:
                    continue
                # For generic locals, common names, or short locals,
                # require domain similarity to avoid false merges
                require_domain_check = (
                    gi or gj
                    or li.lower() in _COMMON_FIRST_NAMES
                    or lj.lower() in _COMMON_FIRST_NAMES
                    or len(li) <= 4
                    or len(lj) <= 4
                )
                if require_domain_check:
                    domain_dist = levenshtein(di, dj)
                    max_dlen = max(len(di), len(dj))
                    domain_threshold = max(3, max_dlen // 3)
                    if domain_dist > domain_threshold:
                        continue
                uf2.union(ci, cj)

    # Convert Union-Find groups to merge map.
    # Also include Strategy 1 merge destinations in UF2 groups to avoid orphans.
    for rep, members in uf2.groups().items():
        if len(members) <= 1:
            continue
        # Include Strategy 1 destinations in the candidate pool
        all_candidates = set(members)
        for m in members:
            if m in new_merges:
                all_candidates.add(new_merges[m])
        best = max(all_candidates, key=lambda c: (_total_count_for_canonical(
            c, canonical_to_originals, nodes_by_id), c))
        for m in all_candidates:
            if m != best:
                new_merges[m] = best

    return new_merges


def _apply_layer_merges(alias_map, merges):
    """Apply a dict of canonical->canonical merges to alias_map. Returns change count.
    Resolves merge chains (A->B->C becomes A->C) before applying."""
    changes = 0
    if not merges:
        return changes
    # Resolve chains: follow each destination to its final target
    resolved = {}
    for src in merges:
        dst = merges[src]
        seen = {src}
        while dst in merges and dst not in seen:
            seen.add(dst)
            dst = merges[dst]
        resolved[src] = dst
    # Apply resolved merges
    for orig_id in list(alias_map.keys()):
        current = alias_map[orig_id]
        if current in resolved:
            alias_map[orig_id] = resolved[current]
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

    # --- Layer 3b: Join Split Local Parts ---
    join_merges = join_split_local_matches(alias_map, all_original_ids)
    changes = _apply_layer_merges(alias_map, join_merges)
    layer_stats.append(("Layer 3b: Join Split Locals", changes))

    # --- Layer 3c: Prefix Stripping ---
    prefix_merges = prefix_strip_matches(alias_map)
    changes = _apply_layer_merges(alias_map, prefix_merges)
    layer_stats.append(("Layer 3c: Prefix Stripping", changes))

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

    # --- Layer 7: Same-Name Merging ---
    # Merge nodes with identical display names within the same domain.
    # Final safety net for duplicates that slipped through earlier layers.
    same_name_merges = _same_name_merge(alias_map, nodes_by_id)
    changes = _apply_layer_merges(alias_map, same_name_merges)
    layer_stats.append(("Layer 7: Same-Name Merge", changes))

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
        # Clean the best ID (structural + domain normalization + local OCR cleanup)
        best_id = structural_cleanup(best_node["id"])
        best_id = apply_domain_normalization(best_id)
        # Apply conservative OCR cleanup to the local part for display
        if '@' in best_id:
            local, domain = best_id.split('@', 1)
            local = ocr_clean_local_for_display(local)
            best_id = f"{local}@{domain}"
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
