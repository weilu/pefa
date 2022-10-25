import requests
import tabula
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlsplit
from pathlib import Path
import PyPDF2
import re
import glob

# report language and primary, secondary, and tertiary keywords to use for table detection
# TODO: deal with non-English docs
config = {
    'English': ("(?:Calculation(?:s)?|Data) (?:.* )?pi",
                ['budg(?:et)?', '(?:actu(?:al)?)'],
                ['data for year', 'deviation', 'administrative']),
    'French': ("(?:Calcul(?:s)?|données|Composition des dépenses effectives) (?:.* )?pi",
               ['(?:prévu|Budg)', '(?:réalis|Ajusté|adjusted)'],
               ["(?:Données pour (?:(?:l’)?année|l'exercice)|Data of year)", 'administra']),
}

def get_pdf_file_path(link_to_content, language, country):
    parts = urlsplit(link_to_content)
    node_number = parts.path.split('/')[-1]
    return f'data/pdfs/{language}_{country}_{node_number}.pdf'

def download_pdf(link_to_content, language, country):
    download_file_path = get_pdf_file_path(link_to_content, language, country)
    if Path(download_file_path).exists():
        print(f'{download_file_path} already exist, skipping')
        return
    req = requests.get(link_to_content)
    soup = BeautifulSoup(req.content, 'html.parser')
    href_path = soup.find("a", text="Download PDF")['href']
    parts = urlsplit(link_to_content)
    base_url = f"{parts.scheme}://{parts.netloc}"
    pdf_url = f'{base_url}{href_path}'
    pdf_req = requests.get(pdf_url)
    with open(download_file_path, 'wb') as f:
        f.write(pdf_req.content)

def page_has_table(pdf_path, page):
    return len(tabula.read_pdf(pdf_path, pages=page)) > 0

def find_tables(language, only_pdf=None):
    keyword, secondary_keywords, tertiary_keywords = config[language]
    for report in sorted(glob.glob(f'data/pdfs/{language}_*.pdf')):
        if only_pdf and report != only_pdf:
            continue
        obj = PyPDF2.PdfFileReader(report)
        num_pages = obj.getNumPages()
        start_page = num_pages // 3 * 2 # assume the annex is in the last third of all pages
        print(f"Searching report: {report}, starting at page ({start_page}/{num_pages})")
        candidates = [] # list of start page number and text content
        for i in range(start_page, num_pages):
            page = obj.getPage(i)
            text = page.extractText()
            if re.search(keyword, text, flags=re.IGNORECASE):
                secondary_founds = list(re.search(sk, text, flags=re.IGNORECASE) for sk in secondary_keywords)
                found = all(secondary_founds)
                if found:
                    candidates.append((i, text))
                else: # try the next page
                    j = i+1
                    if j >= num_pages-1:
                        continue
                    next_page = obj.getPage(j)
                    next_text = next_page.extractText()
                    found_on_next_page = all(re.search(sk, next_text, flags=re.IGNORECASE) for sk in secondary_keywords)
                    if found:
                        candidates.append((j, next_text))
        start_identified = False
        if len(candidates) == 1:
            print(f"    (only candidate) table start on Page: {candidates[0][0]+1}") # 0 index, so +1 for human
            start_identified = True
        elif len(candidates) > 1:
            for page, text in candidates:
                found = any(re.search(key, text, flags=re.IGNORECASE) for key in tertiary_keywords)
                if found:
                    print(f"   (filtered candidate) table start on Page: {page+1}") # 0 index, so +1 for human
                    start_identified = True
                    break
        if not start_identified:
            # Try again requiring all of secondary and tertiary keywords to be present
            for i in range(start_page, num_pages):
                page = obj.getPage(i)
                text = page.extractText()
                keys = secondary_keywords + tertiary_keywords
                results = list(re.search(key, text, flags=re.IGNORECASE) for key in keys)
                secondary_tertiary_found = all(results)
                if secondary_tertiary_found:
                    print(f"    (second chance) table start on Page: {i+1}") # 0 index, so +1 for human
                    start_identified = True
                    break
            if not start_identified:
                print(f'[WARNING] start page not found for {report}!!! {len(candidates)} candidates: {[c[0]+1 for c in candidates]}')


meta_df = pd.read_csv('data/pefa-assessments.csv')
meta_df_to_process = meta_df[(meta_df.Type == 'National') & (meta_df.Availability == 'Public') & (meta_df.Framework == '2016 Framework')]

# takes a few minutes (<5min) to complete
for index, row in meta_df_to_process.iterrows():
    download_pdf(row['Link to Content'], row['Language'], row['Country'])

find_tables('English')
find_tables('French')


# df = tabula.read_pdf("data/pdfs/711.pdf", pages='164')[0]
# df.to_csv('data/afk_2014.csv')
