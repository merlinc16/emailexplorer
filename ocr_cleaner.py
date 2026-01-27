#!/usr/bin/env python3
"""
OCR Error Cleaner for toxic_docs MongoDB collection.

Based on analysis of 500 document samples, this script fixes common OCR errors
using regex patterns and SymSpell spell checking. NO API CALLS - runs locally.

Usage:
    python ocr_cleaner.py --dry-run        # Preview changes without modifying DB
    python ocr_cleaner.py --apply          # Apply fixes to MongoDB
    python ocr_cleaner.py --sample 100     # Test on 100 random docs first
"""

import re
import argparse
from collections import Counter
from typing import Optional

# Optional: Install symspellpy for advanced spell checking
# pip install symspellpy
try:
    from symspellpy import SymSpell, Verbosity
    HAS_SYMSPELL = True
except ImportError:
    HAS_SYMSPELL = False
    print("Note: symspellpy not installed. Install with: pip install symspellpy")
    print("Falling back to regex-only cleaning.\n")

from pymongo import MongoClient


# =============================================================================
# OCR ERROR PATTERNS (discovered from 500-doc sample analysis)
# =============================================================================

# 1. Control character cleanup
CONTROL_CHAR_PATTERN = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]')

# 2. Form feed to paragraph break
FORM_FEED_PATTERN = re.compile(r'\f+')

# 3. Hyphenated line breaks (word- \n continuation)
HYPHEN_LINEBREAK = re.compile(r'(\w{2,})-\s*\n\s*(\w{2,})')

# 4. Multiple spaces to single
MULTI_SPACE = re.compile(r'  +')

# 5. Missing space after punctuation (but not decimals/times)
MISSING_SPACE_AFTER_PUNCT = re.compile(r'([.!?])([A-Z])')

# 6. Space before punctuation (but preserve newlines before periods in lists)
SPACE_BEFORE_PUNCT = re.compile(r' +([.,;:!?])(?!\d)')

# 7. Broken common words (discovered patterns)
BROKEN_WORDS = [
    (re.compile(r'\bth e\b', re.I), 'the'),
    (re.compile(r'\bt he\b', re.I), 'the'),
    (re.compile(r'\ba nd\b', re.I), 'and'),
    (re.compile(r'\ban d\b', re.I), 'and'),
    (re.compile(r'\bw ith\b', re.I), 'with'),
    (re.compile(r'\bwi th\b', re.I), 'with'),
    (re.compile(r'\bo f\b', re.I), 'of'),
    (re.compile(r'\bt o\b', re.I), 'to'),
    (re.compile(r'\bi n\b'), 'in'),  # Case sensitive - "I n" might be intentional
    (re.compile(r'\bi s\b'), 'is'),
    (re.compile(r'\bw as\b', re.I), 'was'),
    (re.compile(r'\bha ve\b', re.I), 'have'),
    (re.compile(r'\bh ave\b', re.I), 'have'),
    (re.compile(r'\bbe en\b', re.I), 'been'),
    (re.compile(r'\bb een\b', re.I), 'been'),
    (re.compile(r'\bfr om\b', re.I), 'from'),
    (re.compile(r'\bf rom\b', re.I), 'from'),
    (re.compile(r'\bth at\b', re.I), 'that'),
    (re.compile(r'\btha t\b', re.I), 'that'),
    (re.compile(r'\bth is\b', re.I), 'this'),
    (re.compile(r'\bthi s\b', re.I), 'this'),
    (re.compile(r'\bwer e\b', re.I), 'were'),
    (re.compile(r'\bw ere\b', re.I), 'were'),
    (re.compile(r'\bwhe n\b', re.I), 'when'),
    (re.compile(r'\bw hen\b', re.I), 'when'),
    (re.compile(r'\bwhi ch\b', re.I), 'which'),
    (re.compile(r'\bw hich\b', re.I), 'which'),
    (re.compile(r'\bthe ir\b', re.I), 'their'),
    (re.compile(r'\bth eir\b', re.I), 'their'),
    (re.compile(r'\bwou ld\b', re.I), 'would'),
    (re.compile(r'\bw ould\b', re.I), 'would'),
    (re.compile(r'\bcou ld\b', re.I), 'could'),
    (re.compile(r'\bc ould\b', re.I), 'could'),
    (re.compile(r'\bshou ld\b', re.I), 'should'),
    (re.compile(r'\bsh ould\b', re.I), 'should'),
    (re.compile(r'\babo ut\b', re.I), 'about'),
    (re.compile(r'\bab out\b', re.I), 'about'),
    (re.compile(r'\bthe se\b', re.I), 'these'),
    (re.compile(r'\bth ese\b', re.I), 'these'),
    (re.compile(r'\bthe re\b', re.I), 'there'),
    (re.compile(r'\bth ere\b', re.I), 'there'),
    (re.compile(r'\bwhe re\b', re.I), 'where'),
    (re.compile(r'\bw here\b', re.I), 'where'),
    (re.compile(r'\bof f\b', re.I), 'off'),
]

# 8. Broken suffixes
BROKEN_SUFFIXES = [
    (re.compile(r'\b(\w{2,})in g\b'), r'\1ing'),
    (re.compile(r'\b(\w{2,})i ng\b'), r'\1ing'),
    (re.compile(r'\b(\w{2,})tio n\b'), r'\1tion'),
    (re.compile(r'\b(\w{2,})ti on\b'), r'\1tion'),
    (re.compile(r'\b(\w{2,})t ion\b'), r'\1tion'),
    (re.compile(r'\b(\w{2,})me nt\b'), r'\1ment'),
    (re.compile(r'\b(\w{2,})m ent\b'), r'\1ment'),
    (re.compile(r'\b(\w{2,})men t\b'), r'\1ment'),
    (re.compile(r'\b(\w{2,})ne ss\b'), r'\1ness'),
    (re.compile(r'\b(\w{2,})n ess\b'), r'\1ness'),
    (re.compile(r'\b(\w{2,})nes s\b'), r'\1ness'),
    (re.compile(r'\b(\w{2,})ly\b'), r'\1ly'),  # broken -ly
]

# 9. Character confusion fixes (HIGH CONFIDENCE - verified in sample)
# These were actually found in your documents
CHAR_CONFUSION_VERIFIED = [
    # h → b confusion (17+ instances of 'tbe')
    (re.compile(r'\btbe\b'), 'the'),
    (re.compile(r'\bTbe\b'), 'The'),
    (re.compile(r'\bTBE\b'), 'THE'),

    # h → li confusion (found 'tlie', 'tiie')
    (re.compile(r'\btlie\b'), 'the'),
    (re.compile(r'\bTlie\b'), 'The'),
    (re.compile(r'\btiie\b'), 'the'),
    (re.compile(r'\bTiie\b'), 'The'),
    (re.compile(r'\bwlien\b'), 'when'),
    (re.compile(r'\bWlien\b'), 'When'),
    (re.compile(r'\bwhicli\b'), 'which'),
    (re.compile(r'\bWhicli\b'), 'Which'),
    (re.compile(r'\bwitli\b'), 'with'),
    (re.compile(r'\bWithi\b'), 'With'),
    (re.compile(r'\botlier\b'), 'other'),
    (re.compile(r'\bOtlier\b'), 'Other'),
    (re.compile(r'\bliave\b'), 'have'),
    (re.compile(r'\bLiave\b'), 'Have'),
    (re.compile(r'\bliis\b'), 'his'),
    (re.compile(r'\bLiis\b'), 'His'),

    # e → c confusion (found 'shcrwin', 'thc', 'lcvcm')
    (re.compile(r'\bshcrwin\b', re.I), 'sherwin'),
    (re.compile(r'\bthc\b'), 'the'),
    (re.compile(r'\bThc\b'), 'The'),
    (re.compile(r'\bTHC\b'), 'THE'),
    (re.compile(r'\blcvcl\b', re.I), 'level'),
    (re.compile(r'\blcvcm\b', re.I), 'level'),  # severe corruption

    # rn → m confusion (found 'rnay', 'frorn')
    (re.compile(r'\brnay\b'), 'may'),
    (re.compile(r'\bRnay\b'), 'May'),
    (re.compile(r'\brnust\b'), 'must'),
    (re.compile(r'\bRnust\b'), 'Must'),
    (re.compile(r'\brnore\b'), 'more'),
    (re.compile(r'\bRnore\b'), 'More'),
    (re.compile(r'\brnake\b'), 'make'),
    (re.compile(r'\bRnake\b'), 'Make'),
    (re.compile(r'\bfrorn\b'), 'from'),
    (re.compile(r'\bFrorn\b'), 'From'),
    (re.compile(r'\bsarne\b'), 'same'),
    (re.compile(r'\bSarne\b'), 'Same'),
    (re.compile(r'\btiine\b'), 'time'),
    (re.compile(r'\bTiine\b'), 'Time'),
    (re.compile(r'\bnarnes\b'), 'names'),
    (re.compile(r'\bNarnes\b'), 'Names'),
    (re.compile(r'\bnurnber\b'), 'number'),
    (re.compile(r'\bNurnber\b'), 'Number'),

    # 1 → l confusion in words (found 'responsibi1ity', 'capab1e')
    (re.compile(r'\bresponsibi1ity\b', re.I), 'responsibility'),
    (re.compile(r'\bcapab1e\b', re.I), 'capable'),
    (re.compile(r'\bappreciab1y\b', re.I), 'appreciably'),
    (re.compile(r'\bisn1t\b', re.I), "isn't"),

    # ft → ff (found 'conftdfnttal' → 'confidential')
    (re.compile(r'\bconftdfnttal\b', re.I), 'confidential'),
    (re.compile(r'\bconftdential\b', re.I), 'confidential'),
    (re.compile(r'\bconfldential\b', re.I), 'confidential'),
    (re.compile(r'\bconfidcntial\b', re.I), 'confidential'),

    # Missing space in common compounds (found 'ofthe', 'ofthis')
    (re.compile(r'\bofthe\b'), 'of the'),
    (re.compile(r'\bofthis\b'), 'of this'),
    (re.compile(r'\bofthese\b'), 'of these'),
    (re.compile(r'\binthe\b'), 'in the'),
    (re.compile(r'\btothe\b'), 'to the'),
    (re.compile(r'\bforthe\b'), 'for the'),
    (re.compile(r'\bandthe\b'), 'and the'),
    (re.compile(r'\bonthe\b'), 'on the'),
    (re.compile(r'\batthe\b'), 'at the'),
]

# 10. Ligature fixes (fi, fl, ff broken)
LIGATURE_FIXES = [
    (re.compile(r'\bf i(\w+)\b'), r'fi\1'),  # 'f irst' → 'first'
    (re.compile(r'\bf l(\w+)\b'), r'fl\1'),  # 'f low' → 'flow'
    (re.compile(r'(\w)f f(\w)'), r'\1ff\2'),  # 'e f fect' → 'effect'
]


def clean_text(text: str, use_symspell: bool = False, sym_spell: Optional['SymSpell'] = None) -> tuple[str, dict]:
    """
    Clean OCR errors from text.

    Returns:
        tuple: (cleaned_text, stats_dict)
    """
    if not text:
        return text, {}

    original = text
    stats = Counter()

    # 1. Remove control characters (except newlines and tabs)
    text, n = CONTROL_CHAR_PATTERN.subn('', text)
    stats['control_chars_removed'] += n

    # 2. Convert form feeds to double newlines
    text, n = FORM_FEED_PATTERN.subn('\n\n', text)
    stats['form_feeds_converted'] += n

    # 3. Fix hyphenated line breaks
    text, n = HYPHEN_LINEBREAK.subn(r'\1\2', text)
    stats['hyphen_breaks_fixed'] += n

    # 4. Fix broken common words
    for pattern, replacement in BROKEN_WORDS:
        text, n = pattern.subn(replacement, text)
        if n > 0:
            stats[f'broken_word_{replacement}'] += n

    # 5. Fix broken suffixes
    for pattern, replacement in BROKEN_SUFFIXES:
        text, n = pattern.subn(replacement, text)
        stats['broken_suffixes_fixed'] += n

    # 6. Fix verified character confusions
    for pattern, replacement in CHAR_CONFUSION_VERIFIED:
        text, n = pattern.subn(replacement, text)
        if n > 0:
            stats[f'char_fix_{replacement}'] += n

    # 7. Fix ligatures
    for pattern, replacement in LIGATURE_FIXES:
        text, n = pattern.subn(replacement, text)
        stats['ligatures_fixed'] += n

    # 8. Fix spacing issues
    text, n = MULTI_SPACE.subn(' ', text)
    stats['multi_spaces_fixed'] += n

    text, n = MISSING_SPACE_AFTER_PUNCT.subn(r'\1 \2', text)
    stats['missing_spaces_added'] += n

    # 9. Optional: SymSpell for remaining errors
    if use_symspell and sym_spell and HAS_SYMSPELL:
        words = re.findall(r'\b[a-zA-Z]{4,}\b', text)
        for word in set(words):
            suggestions = sym_spell.lookup(word.lower(), Verbosity.CLOSEST, max_edit_distance=1)
            if suggestions and suggestions[0].distance == 1:
                # Only fix if high confidence (frequency much higher)
                if suggestions[0].count > 1000:
                    text = re.sub(rf'\b{re.escape(word)}\b', suggestions[0].term, text)
                    stats['symspell_fixes'] += 1

    stats['total_changes'] = sum(stats.values())
    stats['text_changed'] = text != original

    return text, dict(stats)


def setup_symspell() -> Optional['SymSpell']:
    """Initialize SymSpell with English dictionary."""
    if not HAS_SYMSPELL:
        return None

    import pkg_resources

    sym_spell = SymSpell(max_dictionary_edit_distance=2, prefix_length=7)

    # Try to load dictionary
    dict_path = pkg_resources.resource_filename("symspellpy", "frequency_dictionary_en_82_765.txt")
    try:
        sym_spell.load_dictionary(dict_path, term_index=0, count_index=1)
        print(f"Loaded SymSpell dictionary with {len(sym_spell.words)} words")
        return sym_spell
    except Exception as e:
        print(f"Could not load SymSpell dictionary: {e}")
        return None


def process_documents(dry_run: bool = True, sample_size: Optional[int] = None, use_symspell: bool = False):
    """Process documents in MongoDB."""

    client = MongoClient('localhost', 27017)
    db = client['toxic_docs']
    collection = db['documents']

    # Setup SymSpell if requested
    sym_spell = setup_symspell() if use_symspell else None

    # Build query
    query = {'text': {'$exists': True, '$ne': None, '$ne': ''}}

    if sample_size:
        cursor = collection.aggregate([
            {'$match': query},
            {'$sample': {'size': sample_size}}
        ])
        total = sample_size
    else:
        cursor = collection.find(query)
        total = collection.count_documents(query)

    print(f"\n{'DRY RUN - ' if dry_run else ''}Processing {total:,} documents...")
    print("=" * 60)

    global_stats = Counter()
    docs_changed = 0
    docs_processed = 0

    for doc in cursor:
        docs_processed += 1

        if docs_processed % 10000 == 0:
            print(f"  Processed {docs_processed:,}/{total:,} documents...")

        text = doc.get('text', '')
        if not text:
            continue

        cleaned, stats = clean_text(text, use_symspell=use_symspell, sym_spell=sym_spell)

        if stats.get('text_changed'):
            docs_changed += 1
            for key, val in stats.items():
                if key not in ('text_changed', 'total_changes'):
                    global_stats[key] += val

            if not dry_run:
                collection.update_one(
                    {'_id': doc['_id']},
                    {'$set': {'text': cleaned}}
                )

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)
    print(f"\nDocuments processed: {docs_processed:,}")
    print(f"Documents changed: {docs_changed:,} ({100*docs_changed/docs_processed:.1f}%)")
    print(f"\nFixes by category:")

    for key, val in global_stats.most_common():
        print(f"  {key}: {val:,}")

    if dry_run:
        print("\n⚠️  DRY RUN - no changes were made to the database")
        print("   Run with --apply to apply changes")
    else:
        print("\n✓ Changes have been applied to the database")

    client.close()


def main():
    parser = argparse.ArgumentParser(description='Clean OCR errors in toxic_docs MongoDB collection')
    parser.add_argument('--dry-run', action='store_true', default=True,
                        help='Preview changes without modifying database (default)')
    parser.add_argument('--apply', action='store_true',
                        help='Apply changes to database')
    parser.add_argument('--sample', type=int, default=None,
                        help='Process only N random documents (for testing)')
    parser.add_argument('--symspell', action='store_true',
                        help='Use SymSpell for additional spell checking')

    args = parser.parse_args()

    dry_run = not args.apply

    if args.apply:
        confirm = input("This will modify the database. Type 'yes' to confirm: ")
        if confirm.lower() != 'yes':
            print("Aborted.")
            return

    process_documents(
        dry_run=dry_run,
        sample_size=args.sample,
        use_symspell=args.symspell
    )


if __name__ == '__main__':
    main()
