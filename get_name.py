
import sys, re
with open('page.html', 'r') as f:
    html = f.read()
for pat in [r'<title[^>]*>(.*?)</title>', r'<h1[^>]*>(.*?)</h1>',
            r'og:title.*?content=.([^"]+)', r'"name"\s*:\s*"([^"]+)"']:
    m = re.search(pat, html, re.I|re.S)
    if m:
        print('NAME:', m.group(1).strip())
        break
else:
    print('THIN_HTML len=', len(html))
