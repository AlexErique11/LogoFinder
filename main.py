import json
import concurrent.futures
import random
import csv
import sys
import time
import signal
import base64
import re
import cloudscraper
from bs4 import BeautifulSoup
from pathlib import Path
from urllib.parse import urljoin, urlparse, unquote
from curl_cffi import requests as cffi_requests
from collections import Counter, defaultdict

# try to load SeleniumBase
try:
    from seleniumbase import SB
except ImportError:
    SB = None

RESULTS_FILE = "found_logos.jsonl"  # Found logos
FAILED_FILE = "not_found_logos.jsonl"  # Logos not found
INPUT_FILE = "sites"  # Input list
LOGOS_DIR = Path("logos")  # Image folder

HEADLESS_MODE = True  # User will select if Selenium is Headless or headed (w/wo UI)

LOGOS_DIR.mkdir(parents=True, exist_ok=True)

global_stats = Counter() # counter for the number of logos found/not found
stop_level = 0  # 0=Run, 1=Soft Stop (Phase 2), 2=Hard Stop (Exit)
recovered_domains = set()  # Track sites to remove from failure file


def handle_exit(signum, frame):
    global stop_level
    if stop_level == 0:
        print("\n\n" + "!" * 50)
        print(" Phase 1 Interrupted.")
        print(" (Press Stop again to force quit)")
        print("!" * 50 + "\n")
        stop_level = 1
    elif stop_level == 1:
        print("\n\n" + "!" * 50)
        print(" Force Quitting Program...")
        print("!" * 50 + "\n")
        stop_level = 2
        sys.exit(1)


signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


# Load proxies from CSV
def load_proxies(filename='proxies.csv'):
    proxy_map = defaultdict(list)
    flat_list = []
    file_path = Path(filename)

    if not file_path.exists():
        print(f"Proxy file missing: {filename}")
        return {}, []

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            # Skip the string replacement unless you know you have null bytes
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    proxy_url = f"{row['protocols']}://{row['ip']}:{row['port']}"
                    country = row['country']

                    proxy_map[country].append(proxy_url)
                    flat_list.append(proxy_url)
                except KeyError:
                    continue
    except Exception as e:
        print(f"Error loading proxies: {e}")
        return {}, []

    print(f"[*] Loaded {len(flat_list)} proxies across {len(proxy_map)} countries.")
    return proxy_map, flat_list


PROXY_POOL, ALL_PROXIES = load_proxies()

# Loads processed domains from found_logos.jsonl and not_found_logos.jsonl
def load_processed_domains():
    processed = set()
    log_files = [Path(RESULTS_FILE), Path(FAILED_FILE)]

    for log_path in log_files:
        if not log_path.exists():
            continue

        try:
            with log_path.open('r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    try:
                        data = json.loads(line)
                        if domain := data.get('domain'):
                            processed.add(domain)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"[!] Warning: Could not read {log_path.name}: {e}")

    return processed

# Loads the sites that are targeted by the chosen options:
# 1. Bad Downloads
# 2. Parser Misses
# 3. Network Blocks
# 4. Crashes
# 5. Re-download
def load_targeted_sites(options):
    targets = {}
    selected_opts = {o.strip() for o in options.split(',')}
    print(f"Scanning logs for criteria: {selected_opts}...")

    def check_line(line):
        try:
            data = json.loads(line)
            domain = data.get('domain')
            if not domain:
                return

            logo_status = data.get('logo', '')
            method = data.get('method', '')
            local_path = Path(data.get('local_path')) if data.get('local_path') else None

            if '1' in selected_opts:  # Bad Downloads
                if logo_status and "Not found" not in logo_status and "Error" not in logo_status and not local_path:
                    targets[domain] = line

            if '2' in selected_opts:  # Parser Misses
                if logo_status == "Not found" and "Page Loaded" in method:
                    targets[domain] = line

            if '3' in selected_opts:  # Network Blocks
                if logo_status == "Not found" and any(m in method for m in ["Cloudscraper", "CFFI", "Init"]):
                    targets[domain] = line

            if '4' in selected_opts:  # Crashes
                if any(m in method for m in ["Crash", "Exception"]) or "Error" in logo_status:
                    targets[domain] = line

            if '5' in selected_opts:  # Re-download
                if local_path:
                    targets[domain] = line
                    if local_path.exists():
                        try:
                            local_path.unlink()
                        except OSError:
                            pass
        except json.JSONDecodeError:
            pass

    # selects the files from thhe FAILED_FILE
    if any(o in selected_opts for o in ['1', '2', '3', '4']):
        failed_file_path = Path(FAILED_FILE)
        if failed_file_path.exists():
            print(f"   > Reading from {failed_file_path}...")
            with failed_file_path.open('r', encoding='utf-8') as f:
                for line in f:
                    check_line(line)

    # selects the files in case of re-download from RESULTS_FILE
    if '5' in selected_opts:
        results_file_path = Path(RESULTS_FILE)
        if results_file_path.exists():
            print(f"   > Reading from {results_file_path}...")
            with results_file_path.open('r', encoding='utf-8') as f:
                for line in f:
                    check_line(line)

    return list(targets.values())

# Logs the outcome of each website procesed
def save_result(data, filename):
    try:
        with open(filename, 'a', encoding='utf-8') as f:
            f.write(json.dumps(data) + "\n")
    except Exception as e:
        print(f"Error writing to file: {e}")


# Cleanup for the logs
def cleanup_files():
    found_domains = set()
    best_results = {}

    results_path = Path(RESULTS_FILE)
    failed_path = Path(FAILED_FILE)
    logos_path = Path(LOGOS_DIR)

    # 1. Process RESULTS (keeps the best entry of the site (the one that has a file))
    if results_path.exists():
        try:
            with results_path.open('r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        if dom := data.get('domain'):
                            found_domains.add(dom)

                            # Keep if it's the first time seeing it, or if this new version has a file
                            if dom not in best_results or data.get('local_path'):
                                best_results[dom] = data
                    except json.JSONDecodeError:
                        continue

            with results_path.open('w', encoding='utf-8') as f:
                for data in best_results.values():
                    f.write(f"{json.dumps(data)}\n")
        except Exception as e:
            print(f"[!] Results cleanup failed: {e}")

    # 2. Process FAILURES (removes recovered sites in Phase 2)
    if failed_path.exists():
        try:
            unique_failures = {}
            with failed_path.open('r', encoding='utf-8') as f:
                for line in f:
                    try:
                        data = json.loads(line)
                        dom = data.get('domain')
                        # Only keep if not found elsewhere and not in the recovery set
                        if dom and dom not in found_domains and dom not in recovered_domains:
                            unique_failures[dom] = data
                    except json.JSONDecodeError:
                        continue

            with failed_path.open('w', encoding='utf-8') as f:
                for data in unique_failures.values():
                    f.write(f"{json.dumps(data)}\n")
        except Exception as e:
            print(f"[!] Failures cleanup failed: {e}")

    # 3. Purge Empty Files (deletes files that are probably fake logos)
    if logos_path.is_dir():
        for file in logos_path.iterdir():
            if file.is_file():
                try:
                    # Files < 50 bytes are probably empty
                    if file.stat().st_size < 50:
                        file.unlink()
                except OSError:
                    pass


# Does the following:
# it runs cleanup_files()
# it checks for websites in found_logos that either have no actual image in the logos folder or the path is empty
# it checks for logos in the logos folder that have no website corresponding to them
# it looks for sites that have more than one logo
def deduplicate_and_repair():
    print("\n" + "=" * 50)
    print(" MODE 4: REPOSITORY HEALTH (Sync, Clean, Repair)")
    print("=" * 50)

    print("Step 1: Analyzing Database and Files...")
    cleanup_files()

    db_domains = set()
    missing_entries = []
    results_path = Path(RESULTS_FILE)
    logos_dir = Path(LOGOS_DIR)

    if results_path.exists():
        with results_path.open('r', encoding='utf-8') as f:
            for line in f:
                try:
                    data = json.loads(line)
                    if dom := data.get('domain'):
                        db_domains.add(dom)
                        lp = Path(data.get('local_path', ""))
                        if not data.get('local_path') or not lp.exists():
                            missing_entries.append(line)
                except json.JSONDecodeError:
                    continue

    orphans = []
    file_groups = defaultdict(list)

    if logos_dir.is_dir():
        for file_path in logos_dir.iterdir():
            if not file_path.is_file():
                continue

            domain_name = file_path.stem

            if domain_name in db_domains:
                file_groups[domain_name].append(file_path)
            else:
                orphans.append(file_path)

    actual_duplicates = {k: v for k, v in file_groups.items() if len(v) > 1}

    # Report
    print(f"\n{'-' * 30}\n REPOSITORY HEALTH REPORT\n{'-' * 30}")
    print(f" [DB]   Total Sites Tracked : {len(db_domains)}")
    print(f" [!] SITES MISSING LOGOS    : {len(missing_entries)}")
    print(f" [!] USELESS FILES (Orphans): {len(orphans)}")
    print(f" [!] DUPLICATE SETS         : {len(actual_duplicates)}")
    print(f"{'-' * 30}")

    if not any([missing_entries, orphans, actual_duplicates]):
        print("\n[OK] Repository is perfectly healthy.")
        return

    print("\nActions:\n [1] Delete Orphans\n [2] Download Missing\n [3] Prune Duplicates\n [4] Fix ALL\n [0] Cancel")
    choice = input("\nSelect Action: ").strip()

    if choice in ('1', '4') and orphans:
        print(f"Deleting {len(orphans)} orphans...")
        for p in orphans:
            p.unlink(missing_ok=True)

    if choice in ('3', '4') and actual_duplicates:
        print(f"Pruning {len(actual_duplicates)} duplicate sets...")
        for files in actual_duplicates.values():
            # Sort: SVGs first, then by file size (largest to smallest)
            files.sort(key=lambda p: (0 if p.suffix == '.svg' else 1, -p.stat().st_size))
            # Keep the first one, delete the rest
            for to_delete in files[1:]:
                to_delete.unlink(missing_ok=True)

    if choice in ('2', '4') and missing_entries:
        print(f"Recovering {len(missing_entries)} missing sites...")
        process_sites_fast(missing_entries, max_workers=10, targeted_mode=True)

    print("\nMaintenance Complete.")


# Retries a specific site
def retry_single_site():
    print("\n" + "=" * 50)
    print(" MODE 5: SINGLE SITE FIX (Purge & Retry)")
    print("=" * 50)
    target = input("Enter domain to retry (e.g., site.com): ").strip().lower()
    if not target:
        return

    # 1. Clean Logs (Forget the domain)
    for log_path in [Path(RESULTS_FILE), Path(FAILED_FILE)]:
        if log_path.exists():
            print(f"[*] Removing {target} from {log_path.name}...")
            # Human touch: use a list comprehension to filter lines in one go
            with log_path.open('r', encoding='utf-8') as f:
                lines = [line for line in f if target not in line.lower()]

            with log_path.open('w', encoding='utf-8') as f:
                f.writelines(lines)

    # 2. Delete existing images
    logos_dir = Path(LOGOS_DIR)
    if logos_dir.is_dir():
        print(f"[*] Deleting local files for {target}...")
        # .glob() is much faster and cleaner for finding specific files
        for img_file in logos_dir.glob(f"{target}.*"):
            try:
                img_file.unlink()
                print(f"    Deleted: {img_file.name}")
            except OSError:
                pass

    # 3. Re-scrape
    print(f"[*] Re-scraping {target}...")
    input_data = json.dumps({"domain": target})
    res = process_single_line(input_data, is_retry=False)

    if res and "Not found" not in res.get('logo', '') and res.get('local_path'):
        print(f"    [SUCCESS] Found: {res['logo']}")
        save_result(res, RESULTS_FILE)
    else:
        error_msg = res.get('logo', 'Unknown Error') if res else 'No result'
        print(f"    [FAILURE] {error_msg}")
        if res:
            save_result(res, FAILED_FILE)

# Gets the country of the website, so I can match it with a proxi from that country
def get_country_from_domain(url):
    try:
        domain = url.split("//")[-1].split("/")[0].replace("www.", "")
        tld = domain.split(".")[-1].upper()
        return tld if len(tld) == 2 else None
    except:
        return None


# Checks if the content is an image
def is_valid_image_content(content):
    if not content or len(content) < 50: return False
    head = content[:32]
    if head.startswith(b'\x89PNG\r\n\x1a\n'): return True
    if head.startswith(b'\xff\xd8'): return True
    if head.startswith(b'GIF8'): return True
    if head.startswith(b'RIFF') and b'WEBP' in head[:16]: return True
    if head.startswith(b'\x00\x00\x01\x00'): return True
    start_text = content[:300].decode('utf-8', errors='ignore').lower().strip()
    if '<svg' in start_text: return True
    if '<?xml' in start_text and '<svg' in start_text: return True
    return False

# Checks if the url is worth downloading
# (if it's a valid image and not a pixel placeholder or html or json code)
# (sends a HEAD request to check if the link is active
def verify_link_is_ok(url):
    if not url: return False
    if "R0lGODlhAQAB" in url: return False
    if url.startswith("data:"):
        if "data:," in url or "data:;" in url: return False
        if "image/svg" in url:
            try:
                decoded = unquote(url).lower()
                if "<path" not in decoded and "<rect" not in decoded and "<image" not in decoded: return False
            except:
                pass
        return True

    try:
        r = cffi_requests.head(url, timeout=4, impersonate="chrome110")
        ct = r.headers.get('Content-Type', '').lower()
        if 'text/html' in ct or 'application/json' in ct: return False
        if r.status_code == 200: return True
        if r.status_code == 404: return False
    except:
        pass

    if "logo" in url.lower().split('/')[-1] or "brand" in url.lower(): return True
    return False


# Downloads the logo form the link and saves it in logos/ folder
def download_logo_file(image_url, domain, referer=None, cookies=None, user_agent=None):
    logos_dir = Path(LOGOS_DIR)

    # 1. Handle data URIs (images inside code)
    if image_url.startswith("data:"):
        # Skip placeholders or empty icons
        if "R0lGODlhAQAB" in image_url or len(image_url) < 150:
            return None

        try:
            header, encoded = image_url.split(",", 1)
            # Determine extension from the header
            ext = ".svg" if "image/svg" in header else ".png"
            if "image/jpeg" in header: ext = ".jpg"

            save_path = logos_dir / f"{domain}{ext}"

            # Decode the data
            img_data = base64.b64decode(encoded) if ";base64" in header else unquote(encoded).encode('utf-8')
            save_path.write_bytes(img_data)
            return str(save_path)
        except Exception:
            return None

    # 2. Handle normal URLs
    # Get extension from URL or default to .png
    original_path = urlparse(image_url).path
    ext = Path(original_path).suffix.lower()
    if not ext or len(ext) > 5:
        ext = ".png"

    local_path = logos_dir / f"{domain}{ext}"

    # Browser disguise
    headers = {
        "User-Agent": user_agent or "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/110.0.0.0 Safari/537.36",
        "Referer": referer or f"https://{domain}/",
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8"
    }

    # 3. Execution (try standard first, then the bypass scraper)
    for method in ["cffi", "cloudscraper"]:
        try:
            if method == "cffi":
                r = cffi_requests.get(image_url, headers=headers, cookies=cookies, timeout=15)
            else:
                scraper = cloudscraper.create_scraper()
                if cookies: scraper.cookies.update(cookies)
                r = scraper.get(image_url, headers=headers, timeout=20)

            if r.status_code == 200:
                # Check: is it actually an image?
                ctype = r.headers.get('Content-Type', '').lower()
                if 'text' not in ctype and 'json' not in ctype and is_valid_image_content(r.content):
                    local_path.write_bytes(r.content)
                    return str(local_path)
        except Exception:
            continue

    return None

# Decides if it's worth using selenium or the site is just dead
def should_use_selenium(error_msg):
    error_msg = str(error_msg).lower()

    # These errors mean the site is blocking
    bypass_triggers = ("403", "503", "cloud", "captcha")

    # These errors mean the site is dead
    dead_site_triggers = ("name or service", "connection refused", "timeout")

    if any(keyword in error_msg for keyword in bypass_triggers):
        return True

    if any(keyword in error_msg for keyword in dead_site_triggers):
        return False

    # If we don't know, try the browser.
    return True


# Downloads the website's HTML content
# If one method below fails, it moves to the next
# Starts with a direct connection (no proxi)
# Then tries with a proxi from the website's country
# Tries with some random proxies as backup
# Uses curl_cffi to mimic a real Safari 15.5 browser
def get_page_content_cffi(url, debug=False, is_retry=False):
    # 1. Determine which proxies to try
    test_proxies = [None]  # Always try direct first
    country = get_country_from_domain(url)
    limit = 1 if is_retry else 2

    # Add geo-targeted proxies if available
    if country and country in PROXY_POOL:
        regional = random.sample(PROXY_POOL[country], k=min(limit, len(PROXY_POOL[country])))
        test_proxies.extend(regional)

    # Add a few random ones for good measure
    if ALL_PROXIES:
        test_proxies.extend(random.sample(ALL_PROXIES, k=min(limit, len(ALL_PROXIES))))

    last_error = "No proxies available"

    # 2. Iterate through strategies
    for proxy in test_proxies:
        if stop_level > 1:
            raise Exception("Hard Stop triggered by user")

        proxy_label = f"Proxy ({proxy[:20]}...)" if proxy else "Direct"

        try:
            if debug or is_retry:
                print(f"   > [CFFI] Trying {proxy_label}...", end=" ", flush=True)

            # Standard proxy format for requests
            config = {"http": proxy, "https": proxy} if proxy else None

            # The 'impersonate' argument
            resp = cffi_requests.get(
                url,
                impersonate="safari15_5",
                proxies=config,
                timeout=12,
                allow_redirects=True
            )

            if resp.status_code >= 400:
                raise Exception(f"HTTP {resp.status_code}")

            if debug or is_retry: print("Done.")
            return resp.text, f"CFFI ({proxy_label})", resp.url, resp.cookies, None

        except Exception as e:
            if debug or is_retry: print("Failed.")
            last_error = str(e)

    raise Exception(f"CFFI failed all attempts. Last error: {last_error}")

# Creates a cloudscraper scraper that pretends to be Chrome on Windows (similar process as with normal)
def get_page_content_cloudscraper(url, debug=False, is_retry=False):
    if stop_level > 1:
        raise Exception("Hard Stop")

        # Setup the scraper with a realistic Windows/Chrome disguise
    scraper = cloudscraper.create_scraper(
        browser={'browser': 'chrome', 'platform': 'windows', 'desktop': True}
    )

    # Plan: Direct first, then a limited number of proxies
    proxy_limit = 1 if is_retry else 3
    test_proxies = [None] + (random.sample(ALL_PROXIES, k=min(proxy_limit, len(ALL_PROXIES))) if ALL_PROXIES else [])

    last_err = "Init"

    for proxy in test_proxies:
        if stop_level > 1: break

        label = "Direct" if not proxy else f"Proxy ({proxy[:15]}...)"
        try:
            if debug or is_retry:
                print(f"   > [Cloud] {label}...", end=" ", flush=True)

            proxy_config = {"http": proxy, "https": proxy} if proxy else None
            resp = scraper.get(url, proxies=proxy_config, timeout=15)

            if resp.status_code == 200:
                if debug or is_retry: print("Done.")
                return (
                    resp.text,
                    f"Cloudscraper {label}",
                    resp.url,
                    scraper.cookies.get_dict(),
                    scraper.headers.get('User-Agent')
                )

            last_err = f"HTTP {resp.status_code}"
            if debug or is_retry: print(f"Error {resp.status_code}")

        except Exception as e:
            if debug or is_retry: print("X")
            # Don't overwrite a specific HTTP error with a generic proxy connection error
            if "Proxy" not in str(e) or last_err == "Init":
                last_err = str(e)

    raise Exception(f"Cloudscraper failed: {last_err}")


JS_LOGO_EXTRACTOR_SCRIPT = """
            var logos = document.querySelectorAll('*');
            function isLogoContainer(el) {
                var c = (el.className || '').toString().toLowerCase();
                var i = (el.id || '').toString().toLowerCase();
                return c.includes('logo') || c.includes('brand') || i.includes('logo') || 
                       (el.parentElement && (el.parentElement.className || '').toString().toLowerCase().includes('logo'));
            }
            function extractUrl(style, prop) {
                var val = style.getPropertyValue(prop);
                if (val && val !== 'none' && val.includes('url')) {
                    return val.replace(/^url\\(['"]?/, '').replace(/['"]?\\)$/, '');
                }
                return null;
            }
            for (var i = 0; i < logos.length; i++) {
                var el = logos[i];
                if (!isLogoContainer(el)) continue;
                var style = window.getComputedStyle(el);
                var url = extractUrl(style, 'background-image') || extractUrl(style, 'mask-image') || extractUrl(style, '-webkit-mask-image');
                if (!url) {
                    var before = window.getComputedStyle(el, '::before');
                    var after = window.getComputedStyle(el, '::after');
                    url = extractUrl(before, 'background-image') || extractUrl(before, 'content') ||
                          extractUrl(after, 'background-image') || extractUrl(after, 'content');
                }
                if (url && url.length > 10) {
                    var img = document.createElement('img');
                    img.src = url;
                    img.className = 'selenium-extracted-logo';
                    img.style.display = 'none';
                    document.body.appendChild(img);
                }
            }
            """

# Uses seleniumBase to enter a site exactly like a human would
def get_page_content_selenium(url, debug=False):
    if not SB:
        raise Exception("SeleniumBase is not installed.")
    if stop_level > 1:
        raise Exception("Hard Stop triggered.")

    try:
        print(f"\n   > [Selenium] Launching Chrome...", end=" ", flush=True)

        with SB(uc=True, headless=HEADLESS_MODE, page_load_strategy="eager") as sb:
            sb.open(url)

            # 1. Automatic Security Bypass
            if any(term in sb.get_title() for term in ["Bitdefender", "Just a moment"]):
                sb.sleep(6)  # Wait for shields to drop

            # 2. Extract Hidden CSS Logos
            sb.execute_script(JS_LOGO_EXTRACTOR_SCRIPT)
            sb.sleep(1)

            source = sb.get_page_source()

            if len(source) < 500:
                print("Empty Page.")
                raise Exception("Page too short")

            print("Success!")
            return (
                source,
                "SeleniumBase",
                sb.get_current_url(),
                {c['name']: c['value'] for c in sb.get_cookies()},
                sb.get_user_agent()
            )

    except Exception as e:
        print(f"Browser Error: {str(e)[:20]}")
        raise Exception("Selenium failed")

# Gets the logo from an external api to DuckDuckGo or Google favicons
def get_logo_from_external_api(domain):
    try:
        if cffi_requests.head(f"https://icons.duckduckgo.com/ip3/{domain}.ico", timeout=5).status_code == 200:
            return f"https://icons.duckduckgo.com/ip3/{domain}.ico", "API (DuckDuckGo)", None
    except:
        pass
    try:
        url = f"https://www.google.com/s2/favicons?domain={domain}&sz=128"
        if cffi_requests.head(url, timeout=5).status_code == 200:
            return url, "API (Google)", None
    except:
        pass
    return None, None, None

# Combines everything to get the logo:
# Checks the API's (first if they have priority)
# Tries the fast way (CFFI)
# Tries Cloudscraper
# Tries Selenium
# __________________________
# Once it has the website's code, it searches for the logo in this order:
# 1. Background data (searches for "logo")
# 2. Social Tags
# 3. Headers
# 4. Inline code
# 5. CSS Styles
# 6. Favicons
def get_logo(domain, debug=False, is_retry=False, api_priority=False):
    full_url = f"https://{domain}"
    html_text, protocol, final_url = None, "Unknown", full_url
    session_cookies = None
    session_ua = None
    last_err = "Init"

    # Internal helper to check external shortcuts (Google/DuckDuckGo)
    def try_api():
        if is_retry or debug: return get_logo_from_external_api(domain)
        return None, None, None

    # If 'API First' strategy is chosen, check shortcuts before visiting the site
    if api_priority:
        l, m, r = try_api()
        if l: return l, m, r, None, None

    # PHASE 1: Try the fast 'CFFI' fetcher
    if not html_text:
        try:
            html_text, protocol, final_url, session_cookies, session_ua = get_page_content_cffi(full_url, debug,
                                                                                                is_retry)
        except Exception as e:
            last_err = str(e)

    # PHASE 2: If fast fetcher fails, try the Cloudflare-bypass scraper
    if not html_text and not is_retry:
        try:
            html_text, protocol, final_url, session_cookies, session_ua = get_page_content_cloudscraper(full_url, debug,
                                                                                                        is_retry)
        except Exception as e:
            last_err = str(e)

    # PHASE 3: Selenium
    if not html_text and (is_retry or debug):
        if debug or should_use_selenium(last_err):
            try:
                html_text, protocol, final_url, session_cookies, session_ua = get_page_content_selenium(full_url, debug)
            except:
                pass
        elif debug:
            print(f"   [Skip Selenium] Dead site.")

    # PHASE 4: Parsing the HTML
    if html_text:
        last_err = "Page Loaded, Parsing Failed"
        try:
            soup = BeautifulSoup(html_text, 'html.parser')

            # 1. Look for 'JSON-LD'
            for s in soup.find_all('script', type='application/ld+json'):
                if not s.string: continue
                try:
                    data = json.loads(s.string)

                    # Search inside JSON objects for a 'logo' key
                    def find_logo(obj):
                        if isinstance(obj, dict):
                            if 'logo' in obj: return obj['logo']
                            for v in obj.values():
                                res = find_logo(v)
                                if res: return res
                        elif isinstance(obj, list):
                            for i in obj: res = find_logo(i);
                            if res: return res
                        return None

                    found = find_logo(data)
                    if found:
                        url = found['url'] if isinstance(found, dict) else found

                        # Check: don't accept the image if it's just a link to the homepage
                        clean_logo_url = url.rstrip('/').replace('www.', '').replace('https://', '').replace('http://',
                                                                                                             '')
                        clean_site_url = domain.replace('www.', '')
                        if clean_logo_url == clean_site_url: continue

                        if url and not url.startswith(('http:', 'https:', '//')) and ('www.' in url or domain in url):
                            url = f"https://{url}"

                        final = urljoin(final_url, url)
                        if verify_link_is_ok(
                            final): return final, f"JSON-LD [{protocol}]", final_url, session_cookies, session_ua
                except:
                    continue

            # 2. Look for social media tags (OpenGraph/Twitter)
            for meta in soup.find_all('meta', property=re.compile(r'og:image|twitter:image')):
                url = meta.get('content')
                if url:
                    final = urljoin(final_url, url)
                    if verify_link_is_ok(
                        final): return final, f"Meta ({meta.get('property')}) [{protocol}]", final_url, session_cookies, session_ua

            # 3. Look for Apple icons or standard image links
            for link in soup.find_all('link', rel=re.compile(r'image_src|apple-touch-icon')):
                url = link.get('href')
                if url:
                    final = urljoin(final_url, url)
                    if verify_link_is_ok(
                        final): return final, f"Link ({link.get('rel')}) [{protocol}]", final_url, session_cookies, session_ua

            # 4. Look for 'Inline' SVGs (Images built directly into the code)
            for svg in soup.find_all('svg'):
                parent = svg.parent
                is_logo = False
                for _ in range(3):
                    if parent:
                        check_str = str(parent.attrs).lower()
                        if 'logo' in check_str or 'brand' in check_str or 'header' in str(parent.name):
                            is_logo = True;
                            break
                        parent = parent.parent
                    else:
                        break
                if is_logo:
                    svg_content = str(svg)
                    encoded_svg = base64.b64encode(svg_content.encode('utf-8')).decode('utf-8')
                    final = f"data:image/svg+xml;base64,{encoded_svg}"
                    if verify_link_is_ok(
                        final): return final, f"Inline SVG [{protocol}]", final_url, session_cookies, session_ua

            # 5. Look through all standard images (IMG tags)
            all_imgs = soup.find_all('img')
            for i, img in enumerate(all_imgs):
                potential_urls = []
                for k, v in img.attrs.items():
                    if isinstance(v, str) and ('http' in v or '//' in v):
                        candidates = v.split(',')
                        for cand in candidates:
                            clean_url = cand.strip().split(' ')[0]
                            if clean_url and (
                                    '.png' in clean_url or '.jpg' in clean_url or '.svg' in clean_url or '.webp' in clean_url):
                                potential_urls.append(clean_url)

                # Does the class, ID, or alt text mention 'logo'?
                combined_attrs = (str(img.get('class')) + str(img.get('id')) + str(img.get('alt')) + str(
                    img.get('title'))).lower()
                is_candidate = False
                if 'logo' in combined_attrs or 'brand' in combined_attrs: is_candidate = True

                # If not labeled, check if the parent container is the site header
                if not is_candidate:
                    parent = img.parent
                    for _ in range(3):
                        if parent:
                            p_attrs = (str(parent.get('class')) + str(parent.get('id'))).lower()
                            if 'logo' in p_attrs or 'brand' in p_attrs or 'navbar-brand' in p_attrs:
                                is_candidate = True;
                                break
                            parent = parent.parent
                        else:
                            break

                # Is 'logo' in the filename or is it one of the first 5 images?
                src = img.get('src', '')
                if not is_candidate and 'logo' in src.lower(): is_candidate = True

                if not is_candidate and i < 5:
                    clean_domain = domain.split('.')[0]
                    if clean_domain in src.lower() or clean_domain in combined_attrs: is_candidate = True

                if is_candidate:
                    for raw_url in potential_urls:
                        final = urljoin(final_url, raw_url)
                        if verify_link_is_ok(
                            final): return final, f"IMG (Rescued) [{protocol}]", final_url, session_cookies, session_ua
                    if src:
                        final = urljoin(final_url, src)
                        if verify_link_is_ok(
                            final): return final, f"IMG [{protocol}]", final_url, session_cookies, session_ua

            # 6. Check for CSS background images found by Selenium
            for img in soup.find_all('img', class_='selenium-extracted-logo'):
                src = img.get('src')
                if src:
                    final = urljoin(final_url, src)
                    return final, f"CSS Background [{protocol}]", final_url, session_cookies, session_ua

            # 7. Final backup: standard favicon icon
            icon = soup.find('link', rel=lambda x: x and 'icon' in x.lower())
            if icon and icon.get('href'):
                final = urljoin(final_url, icon.get('href'))
                if verify_link_is_ok(
                    final): return final, f"Favicon [{protocol}]", final_url, session_cookies, session_ua
        except:
            pass

    # If we didn't check APIs at the start, check them now
    if not api_priority:
        l, m, r = try_api()
        if l: return l, m, r, None, None

    return "Not found", f"Failed: {last_err}", None, None, None

# Handles reading the lines from the input file, calling the functions to search the logo, save it, create report, etc
def process_single_line(line, is_retry=False, api_priority=False):
    # Skip empty lines or stop immediately if a 'Hard Stop' was triggered
    if not line.strip() or stop_level > 1: return None
    # Wait a random moment (0.5 to 1.5 seconds) so we don't spam the server
    if not is_retry: time.sleep(random.uniform(0.5, 1.5))
    try:
        site_data = json.loads(line)
        domain = site_data.get('domain')
        if domain:
            logo, method, ref, cookies, ua = get_logo(domain, is_retry=is_retry, api_priority=api_priority)
            local = None
            if logo and "Not found" not in logo:
                local = download_logo_file(logo, domain, referer=ref, cookies=cookies, user_agent=ua)
            return {'domain': domain, 'logo': logo, 'local_path': local, 'method': method}
    except Exception as e:
        return {'domain': json.loads(line).get('domain'), 'logo': f"Error: {e}", 'method': 'Crash', 'local_path': None}
    return None


def process_sites_fast(input_source, max_workers=10, targeted_mode=False, api_priority_mode=False):
    lines_to_process = []

    if targeted_mode:
        lines_to_process = input_source
        print(f"Targeted Mode: {len(lines_to_process)} sites selected.")
    else:
        source_path = Path(input_source)
        if not source_path.exists():
            return

        # Load successfully processed domains to avoid duplicates
        processed = load_processed_domains()
        print(f"Skipping {len(processed)} previously processed domains.")

        with source_path.open('r', encoding='utf-8') as f:
            for line in f:
                try:
                    # Only add to the queue if we haven't seen this domain before
                    if json.loads(line).get('domain') not in processed:
                        lines_to_process.append(line)
                except json.JSONDecodeError:
                    continue

    if not lines_to_process:
        print("No sites to process.")
        return

    print(f"Starting Phase 1 on {len(lines_to_process)} sites...")
    failed_entries = []

    # PHASE 1

    # Creates 'max_workers' number of parallel threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_line = {
            executor.submit(process_single_line, line, is_retry=False, api_priority=False): line
            for line in lines_to_process
        }

        try:
            for future in concurrent.futures.as_completed(future_to_line):
                # Stop: stop_level 1
                if stop_level >= 1:
                    executor.shutdown(wait=False, cancel_futures=True)
                    break

                line = future_to_line[future]
                try:
                    res = future.result()
                    if res:
                        logo_status = res.get('logo', '')
                        # Found logo and downloaded the file
                        if "Not found" not in logo_status and "Error" not in logo_status and res.get('local_path'):
                            print(f"[{res['method']}] {res['domain']}")
                            save_result(res, RESULTS_FILE)
                            global_stats["Found"] += 1
                            if targeted_mode:
                                recovered_domains.add(res['domain'])

                        # Found the URL but the download was blocked
                        elif "Not found" not in logo_status and "Error" not in logo_status and not res.get(
                                'local_path'):
                            print(f"[x] {res['domain']} -> Found URL but Download Failed")
                            save_result(res, FAILED_FILE)
                            global_stats["Download Failed"] += 1

                        # Categorize as a failure for Phase 2
                        else:
                            print(f"[x] {res['domain']} -> {res['method']}")
                            save_result(res, FAILED_FILE)
                            failed_entries.append((line, res.get('method', 'Unknown')))
                            global_stats["Failed"] += 1
                except Exception:
                    failed_entries.append((line, "Exception"))
        except KeyboardInterrupt:
            executor.shutdown(wait=False, cancel_futures=True)

    # PHASE 2 (One-by-one with Selenium)
    if failed_entries and stop_level < 2:
        print(f"\n{'=' * 50}\nPHASE 2: SMART RETRY ({len(failed_entries)} sites)\n{'=' * 50}")
        for i, (line, err) in enumerate(failed_entries):
            if stop_level >= 2: break

            domain = json.loads(line).get('domain')

            # Only retry if the site isn't dead
            if should_use_selenium(err):
                print(f"\n[{i + 1}/{len(failed_entries)}] RETRY: {domain} ...", end="", flush=True)
                # Use Selenium/Scraper with retry flag set to True
                res = process_single_line(line, is_retry=True, api_priority=api_priority_mode)

                if res and "Not found" not in res.get('logo', ''):
                    print(f"\n   -> [RECOVERED] {res['method']}")
                    save_result(res, RESULTS_FILE)
                    global_stats["Recovered"] += 1
                    recovered_domains.add(domain)
                else:
                    print("\n   -> [DEAD]")
                print("-" * 30)
            else:
                print(f"[{i + 1}] SKIP: {domain} (Dead: {err})")

    cleanup_files()
    print(f"\nDone. Found: {global_stats['Found']} (+{global_stats['Recovered']} recovered)")


if __name__ == "__main__":
    print("MODE: 1. Batch Process | 2. Single Debug | 3. Targeted Retry | 4. Repo Maintenance | 5. Single Site Fix")
    choice = input("Choice: ").strip()

    if choice == '1':
        vis = input("Show Browser in Phase 2? (y/n): ").lower()
        HEADLESS_MODE = (vis != 'y')
        print("\nPhase 2 Strategy:\n1. Standard (Selenium -> API)\n2. API First (API -> Selenium)")
        strat = input("Choice: ").strip()
        process_sites_fast(INPUT_FILE, max_workers=10, api_priority_mode=(strat == '2'))

    elif choice == '2':
        HEADLESS_MODE = False
        target = input("Domain: ").strip().replace("https://", "").replace("http://", "").split("/")[0]
        print(f"\n--- DEBUGGING {target} ---")
        l, m, r, c, u = get_logo(target, debug=True, is_retry=True)
        if l and "Not found" not in l:
            path = download_logo_file(l, target, r, cookies=c, user_agent=u)
            print(f"Local Path: {path}")
        print(f"FINAL: {l} ({m})")

    elif choice == '3':
        print("\nWhich failures to retry? (e.g. '1,3')")
        print("1. Bad Downloads (Found URL but failed to save)")
        print("2. Parser Misses (Site loaded, but logo not found)")
        print("3. Network Blocks (Cloudscraper/CFFI failed)")
        print("4. Crashes (Script errors)")
        print("5. Re-download (Retry sites that HAVE a local_path)")
        sub = input("Select: ").strip()
        vis = input("Show Browser in Phase 2? (y/n): ").lower()
        HEADLESS_MODE = (vis != 'y')
        print("\nRetry Strategy:\n1. Standard\n2. API First")
        strat = input("Choice: ").strip()
        lines = load_targeted_sites(sub)
        process_sites_fast(lines, max_workers=10, targeted_mode=True, api_priority_mode=(strat == '2'))

    elif choice == '4':
        deduplicate_and_repair()

    elif choice == '5':
        retry_single_site()

    sys.exit(0)