import json
import os

with open('grouped_websites_strict.json', 'r', encoding='utf-8') as f:
    grouped_data = json.load(f)

error_domains = set()
for group in grouped_data.get('groups', []):
    if group.get('type') == 'error_group':
        error_domains.update(group.get('domains', []))

found_remaining = []
not_found_extracted = []

if os.path.exists('found_logos.jsonl'):
    with open('found_logos.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            clean_line = line.strip()
            if not clean_line:
                continue
            try:
                entry = json.loads(clean_line)
                if entry.get('domain') in error_domains:
                    not_found_extracted.append(clean_line)
                else:
                    found_remaining.append(clean_line)
            except json.JSONDecodeError:
                found_remaining.append(clean_line)

with open('found_logos.jsonl', 'w', encoding='utf-8') as f:
    for line in found_remaining:
        f.write(line + '\n')

with open('not_found_logos.jsonl', 'a', encoding='utf-8') as f:
    for line in not_found_extracted:
        f.write(line + '\n')