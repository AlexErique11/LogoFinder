import json
import os
import requests
import imagehash
import concurrent.futures
import io
import logging
from pathlib import Path
from PIL import Image, ImageOps, UnidentifiedImageError, ImageStat
from collections import defaultdict
from urllib.parse import urlparse
from svglib.svglib import svg2rlg
from reportlab.graphics import renderPM

LOGOS_DIR = Path("logos")
INPUT_FILE = Path("found_logos.jsonl")
OUTPUT_FILE = Path("grouped_websites_strict.json")
OUTPUT_SHORT_FILE = Path("grouped_short.json")

HASH_SIZE = 16
SIMILARITY_THRESHOLD = 5
MAX_WORKERS = os.cpu_count() # number of parallel workers

# Mute not critical warnings
logging.getLogger('svglib').setLevel(logging.ERROR)
logging.getLogger('reportlab').setLevel(logging.ERROR)
logging.getLogger('xml').setLevel(logging.ERROR)

LOGOS_DIR.mkdir(parents=True, exist_ok=True)

# Downloads the logos if they are missing
def download_image_if_missing(url, domain):
    # stops if the link is not valid
    if not url or "http" not in url:
        return None

    try:
        path = urlparse(url).path
        ext = Path(path).suffix.lower()
        # gets the extension or png by default
        if not ext or len(ext) > 5:
            ext = ".png"

        local_path = LOGOS_DIR / f"{domain}{ext}"

        # deletes the file if it is empty
        if local_path.exists() and local_path.stat().st_size == 0:
            local_path.unlink()

        # if the logo exists it returns
        if local_path.exists():
            return local_path

        # tries to download the logo acting like a Mozilla browser
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            local_path.write_bytes(response.content)
            return local_path

    except:
        pass

    return None

def load_and_preprocess_image(local_path):
    img = None
    try:
        if local_path.stat().st_size == 0:
            return None, "Empty File (0 bytes)"

        is_actually_svg = False
        is_html = False
        try:
            with local_path.open('rb') as f:
                header = f.read(200).lower()

                if b'<svg' in header or b'<?xml' in header:
                    is_actually_svg = True

                # If it's a html file it will return null and an error later
                if b'<html' in header or b'<!doctype html' in header:
                    is_html = True
        except:
            pass

        if is_html:
            return None, "Invalid: File is HTML (Access Denied)"

        if local_path.suffix.lower() == ".svg" or is_actually_svg:
            try:
                # Read SVG text and fix common scaling issues
                svg_text = local_path.read_text(encoding='utf-8', errors='ignore')

                if '="auto"' in svg_text:
                    svg_text = svg_text.replace('width="auto"', 'width="100%"')
                    svg_text = svg_text.replace('height="auto"', 'height="100%"')

                f_io = io.BytesIO(svg_text.encode('utf-8'))
                drawing = svg2rlg(f_io)
                if not drawing:
                    return None, "SVG Parsing Failed"

                # Draws the image on a neutral 0x808080 gray background
                img = renderPM.drawToPIL(drawing, bg=0x808080)
            except Exception as e:
                return None, f"SVG Error: {str(e)}"

        else:
            try:
                img = Image.open(local_path)
                img.load()
            except UnidentifiedImageError:
                return None, "PIL Failed: Unknown/Corrupt Format"
            except Exception as e:
                return None, f"Image Error: {str(e)}"

        try:
            # Handle transparency (Alpha channel) in RGBA, LA, or Palette modes
            if img.mode in ('RGBA', 'LA') or (img.mode == 'P' and 'transparency' in img.info):
                img = img.convert('RGBA')
                alpha = img.split()[-1]

                # Fail if the image is entirely transparent
                if alpha.getextrema()[1] == 0:
                    return None, "Blank Image (Fully Transparent)"

                # Calculate average brightness to determine best background color
                stat = ImageStat.Stat(img.convert('L'), mask=alpha)
                if stat.count[0] > 0:
                    avg_brightness = stat.mean[0]
                else:
                    avg_brightness = 255

                # Use black background for bright logos, white for dark logos
                bg_color = (0, 0, 0) if avg_brightness > 150 else (255, 255, 255)

                bg = Image.new("RGB", img.size, bg_color)
                bg.paste(img, mask=alpha)
                img = bg
            else:
                # Standardize non-transparent images to RGB
                img = img.convert("RGB")

            img = img.convert("L")

            # Ensure the image isn't just a single color
            extrema = img.getextrema()
            if extrema and (extrema[1] - extrema[0] < 5):
                if local_path.suffix.lower() == ".svg" or is_actually_svg:
                    pass
                else:
                    return None, "Blank Image (Solid Color)"

            # Find the bounding box of the non-background content
            inverted = ImageOps.invert(img)
            bbox = inverted.getbbox()
            if bbox:
                img = img.crop(bbox)

            return img, "OK"
        except Exception as e:
            return None, f"Processing Error: {str(e)}"

    except Exception as e:
        return None, f"System Error: {str(e)}"

# Uses previous methods to download the logo if missing, add background and then hash the image and prepare it for the final grouping part
def process_single_entry(line):
    result = {
        "status": "error",
        "domain": "unknown",
        "url": None,
        "local_path": None,
        "error": "Unknown"
    }

    try:
        data = json.loads(line)
        domain = data.get('domain')
        url = data.get('logo')

        local_path_str = data.get('local_path')
        local_path = Path(local_path_str) if local_path_str else None

        result['domain'] = domain
        result['url'] = url
        result['local_path'] = local_path_str

        # Try to download the logo if missing
        if not local_path or not local_path.exists():
            local_path = download_image_if_missing(url, domain)
            result['local_path'] = str(local_path) if local_path else None

        # stop and report failure if the logo is still missing
        if not local_path or not local_path.exists():
            result['error'] = "Download Failed / File Missing"
            return result

        # prepare the image
        clean_img, status_msg = load_and_preprocess_image(local_path)

        # Stop if the image is invalid (HTML page or blank)
        if clean_img is None:
            result['error'] = status_msg
            return result

        # Create the fingerprint using Difference Hashing
        img_hash = imagehash.dhash(clean_img, hash_size=HASH_SIZE)

        # Return the success data including the hash objects for grouping
        return {
            "status": "success",
            "domain": domain,
            "url": url,
            "local_path": str(local_path),
            "hash_obj": img_hash,
            "hash_str": str(img_hash)
        }
    except Exception as e:
        result['error'] = f"Crash: {str(e)}"
        return result

# It groups the logos by their fingerprint:
# First it groups all identical fingerprints together and keeps the one as the representative logo
# For all other logos that are not yet grouped they are compared to each group so far
def main():
    print("GROUPING STARTED-")

    if not INPUT_FILE.exists():
        print(f"Error: {INPUT_FILE} not found.")
        return

    # Read lines and filter out empty ones
    with INPUT_FILE.open('r', encoding='utf-8') as f:
        lines = [line for line in f if line.strip()]

    # Parallel Processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        results = list(executor.map(process_single_entry, lines))

    successes = [r for r in results if r['status'] == 'success']
    failures = [r for r in results if r['status'] == 'error']

    print(f"Total Input: {len(lines)}")
    print(f"Valid Logos: {len(successes)}")
    print(f"Errors/Blank: {len(failures)}")

    # Exact Matching
    exact_groups = defaultdict(list)
    for entry in successes:
        exact_groups[entry['hash_str']].append(entry)

    unique_hashes = list(exact_groups.keys())
    hash_objects = {h: exact_groups[h][0]['hash_obj'] for h in unique_hashes}

    # Similarity Clustering
    final_groups = []
    processed_hashes = set()

    for i, pivot_str in enumerate(unique_hashes):
        if pivot_str in processed_hashes:
            continue

        current_group = list(exact_groups[pivot_str])
        processed_hashes.add(pivot_str)
        pivot_obj = hash_objects[pivot_str]

        for j in range(i + 1, len(unique_hashes)):
            candidate_str = unique_hashes[j]
            if candidate_str in processed_hashes:
                continue

            # Compare visual fingerprints
            if (pivot_obj - hash_objects[candidate_str]) <= SIMILARITY_THRESHOLD:
                current_group.extend(exact_groups[candidate_str])
                processed_hashes.add(candidate_str)

        final_groups.append({
            "type": "valid_group",
            "count": len(current_group),
            "domains": sorted([x['domain'] for x in current_group]),
            "example_logo": current_group[0]['url'],
            "all_data": current_group
        })

    # Error Grouping
    if failures:
        err_dict = defaultdict(list)
        for f in failures:
            err_dict[f['error']].append({
                "domain": f['domain'],
                "logo": f['url'],
                "local_path": f['local_path']
            })

        for reason, items in err_dict.items():
            final_groups.append({
                "type": "error_group",
                "reason": reason,
                "count": len(items),
                "domains": sorted([x['domain'] for x in items]),
                "all_data": items
            })

    # Sorting largest groups first
    final_groups.sort(key=lambda x: (x['type'] == 'error_group', -x['count']))

    # save outputs
    output_data = {
        "stats": {"total": len(lines), "processed": len(successes), "failed": len(failures)},
        "groups": final_groups
    }

    with OUTPUT_FILE.open('w', encoding='utf-8') as f:
        json.dump(output_data, f, indent=4)

    short_data = {
        f"Group {i} ({'ERROR: ' + g.get('reason', '') if g['type'] == 'error_group' else 'Valid - ' + str(g['count']) + ' domains'})":
            g['domains']
        for i, g in enumerate(final_groups, 1)
    }

    with OUTPUT_SHORT_FILE.open('w', encoding='utf-8') as f:
        json.dump(short_data, f, indent=4)

    print(f"Grouping complete. Saved results to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()