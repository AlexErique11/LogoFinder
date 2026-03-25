# Logo Scraper & Visual Grouper

##  How It Works

### Phase 1: The Scraper (Cascade Framework)

The scraper (`main.py`) processes a list of domains using a fallback system. If one method fails, it goes to the next one:

1. **Fast HTTP Fetch (`curl_cffi`)**
   - Mimics a modern Safari browser for lightweight HTML retrieval

2. **Anti-Bot Bypass (`cloudscraper`)**
   - Handles Cloudflare and common bot protections

3. **Headless Browser (`SeleniumBase`)**
   - Executes JavaScript to detect dynamically loaded logos
     
4. **External API Shortcuts**
   - Google & DuckDuckGo favicon services for quick results

---

### Search Heuristics

The scraper looks for logos in this order:

- **Structured Data**
  - Extracts from JSON-LD (`ld+json`)
- **Social Tags**
  - `og:image`, Twitter cards
- **Meta Links**
  - `apple-touch-icon`, `image_src`
- **Inline SVGs**
  - Encodes SVGs directly embedded in HTML
- **Heuristic IMG Scan**
  - Finds `<img>` tags with keywords like *logo* or *brand*
- **CSS Backgrounds**
  - Detects images via `background-image` or `mask-image`

---

### Phase 2: Visual Grouping & Deduplication

Handled by `group.py`, grouping logos based on visual similarity.

#### Image Preprocessing
- SVG → raster conversion
- Smart background fill (black/white based on brightness)
- Auto-cropping whitespace

#### Perceptual Hashing
- Uses **dHash** to generate image fingerprints

#### Similarity Clustering
- **Exact Matches** → identical hashes
- **Fuzzy Matches** → Hamming distance ≤ 5

---

## Getting Started

### 1. Installation

```bash
pip install requests imagehash pillow seleniumbase cloudscraper curl_cffi beautifulsoup4 svglib reportlab
```

---

### 2. Usage

#### Scrape Logos

1. Create a file named `sites` with one domain per line  
2. Run:

```bash
python main.py
```

**Modes:**
- `1` → Batch process all sites  
- `3` → Retry failed sites  
- `4` → Cleanup & health check  

---

#### Group Logos

```bash
python group.py
```

Generates:

- `grouped_websites_strict.json`
- `grouped_short.json`

---

## Project Structure

```
main.py                  # Scraper & manager
group.py                 # Image hashing & grouping
logos/                   # Downloaded logos
found_logos.jsonl        # Successful scrape database
grouped_short.json       # Simplified grouping output
```

---

## Key Features

- **Proxy Support**
  - Matches TLDs (.fr, .de, etc.) with local proxies

- **Parallel Processing**
  - Multi-threaded scraping
    
- **Smart Backgrounds**
  - Auto-adjusts contrast for hashing accuracy
