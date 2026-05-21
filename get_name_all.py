
import sys, re
html = sys.stdin.read()
results = {}
patterns = {
    "title": r'<title[^>]*>(.*?)</title>',
    "h1": r'<h1[^>]*>(.*?)</h1>',
    "og:title": r'og:title.*?content=.([^"]+)',
    "json_name": r'"name"\s*:\s*"([^"]+)"'
}
for name, pat in patterns.items():
    m = re.search(pat, html, re.I|re.S)
    if m:
        results[name] = m.group(1).strip()

if results:
    for name, value in results.items():
        print(f'{name}: {value}')
else:
    print('THIN_HTML len=', len(html))
