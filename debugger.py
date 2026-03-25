import json
import os
import random
import time
from selenium import webdriver
from pynput import keyboard

LOGOS_FILE = 'found_logos.jsonl'
HISTORY_FILE = 'verification_history.json'


def load_history():
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                content = f.read().strip()
                if not content:
                    return {}
                return json.loads(content)
        except (json.JSONDecodeError, IOError):
            return {}
    return {}


def save_history(history):
    with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
        json.dump(history, f, indent=4)


def start_verification():
    if not os.path.exists(LOGOS_FILE):
        print(f"Error: {LOGOS_FILE} not found.")
        return

    history = load_history()
    with open(LOGOS_FILE, 'r', encoding='utf-8') as f:
        all_data = [json.loads(line) for line in f if line.strip()]

    data = [entry for entry in all_data if entry.get('domain') not in history]

    if not data:
        print("All sites have already been checked!")
        return

    random.shuffle(data)

    session_results = {
        '0': [],
        '1': [],
        '2': []
    }

    state = {'input': None, 'active': True}

    def on_press(key):
        try:
            if hasattr(key, 'char') and key.char in ['0', '1', '2', 's', 'q']:
                state['input'] = key.char
        except Exception:
            pass

    listener = keyboard.Listener(on_press=on_press)
    listener.start()

    options = webdriver.ChromeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    driver = webdriver.Chrome(options=options)

    mapping = {'0': 'OK', '1': 'Investigate', '2': 'Wrong', 's': 'Skipped'}

    print(f"--- Debugger Started ({len(data)} remaining) ---")
    print("[0] OK | [1] Investigate | [2] Wrong | [s] Skip | [q] Quit")

    try:
        for entry in data:
            if not state['active']:
                break

            domain = entry.get('domain')
            local_path = os.path.abspath(entry.get('local_path')).replace("\\", "/")
            img_url = f"file:///{local_path}"
            site_url = domain if domain.startswith('http') else f"https://{domain}"

            driver.get(img_url)
            try:
                driver.execute_script("document.body.style.backgroundColor = 'gray';")
            except Exception:
                pass
            driver.execute_script(f"window.open('{site_url}', '_blank');")

            state['input'] = None
            print(f"\nChecking: {domain}")

            while state['input'] is None:
                time.sleep(0.1)

            action = state['input']

            if action == 'q':
                state['active'] = False
                print("Action: Quitting...")
                break

            if action in ['0', '1', '2']:
                history[domain] = action
                session_results[str(action)].append(domain)
                print(f"Action: {mapping[str(action)]}")
            elif action == 's':
                print("Action: Skipped")

            main_handle = driver.window_handles[0]
            for handle in driver.window_handles:
                if handle != main_handle:
                    driver.switch_to.window(handle)
                    driver.close()
            driver.switch_to.window(main_handle)

    finally:
        driver.quit()
        listener.stop()
        save_history(history)

    print("\n" + "=" * 40)
    print("SESSION SUMMARY")
    print("=" * 40)

    for code, label in [('0', 'OK'), ('1', 'INVESTIGATE'), ('2', 'WRONG')]:
        print(f"\n{label} ({len(session_results[code])}):")
        for site in session_results[code]:
            print(f" - {site}")

    print("\n" + "=" * 40)

    total_counts = {'0': 0, '1': 0, '2': 0}
    for val in history.values():
        if val in total_counts:
            total_counts[val] += 1

    print("GLOBAL STATS (All Sessions):")
    print(f"OK: {total_counts['0']}")
    print(f"Investigate: {total_counts['1']}")
    print(f"Wrong: {total_counts['2']}")
    print(f"Total Progress: {len(history)} / {len(all_data)}")
    print("=" * 40)


if __name__ == "__main__":
    start_verification()