import requests
import tabula
import pandas as pd
from bs4 import BeautifulSoup
from urllib.parse import urlsplit
from pathlib import Path
import PyPDF2
import re
import glob
import unicodedata
# import camelot


# report language and primary, secondary, and tertiary keywords to use for table detection
config = {
    'English': ("(?:Calculation(?:s)?|Data) (?:.* )?pi",
                ['budg(?:et)?', '(?:actu(?:al)?|Execution)'],
                ['(?:data for (?:the )?year|Data on (?:the functional classification|economic categories){1} for)', 'deviation', '(?:administrative|Functional head)']),
    'French': ("(?:Calcul(?:s)?|données|Composition des dépenses effectives) (?:.* )?pi",
               ['(?:prévu|Budg)', '(?:réalis|Ajusté|adjusted)'],
               ["(?:Données pour (?:(?:l’)?année|l'exercice)|Data of year)", 'administra']),
    'Spanish': ("(?:calcular|datos|D a t o s) (?:.* )?(?:id|i d)",
                ['(?:budget|Inicial)', '(?:actual|Ejecutado)'],
                ['(?:data for year|Año)', '(?:deviation|Desviación)', '(?:administrative|Sectorial|percent)']),
    'Portuguese': ("Anexo 4\. Cálculos das variações para os indicadores PI", [], []),
}
config['Français'] = config['French']

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

def normalize_as_filename(filename):
    return unicodedata.normalize('NFKD', filename)

def find_tables(language, only_pdf=None):
    keyword, secondary_keywords, tertiary_keywords = config[language]
    results = []
    for report in sorted(glob.glob(f"data/pdfs/{normalize_as_filename(language)}_*.pdf")):
        if only_pdf and report != only_pdf:
            continue
        table_start_page = None
        obj = PyPDF2.PdfFileReader(report)
        num_pages = obj.getNumPages()
        start_page = num_pages // 3 * 2 # assume the annex is in the last third of all pages
        print(f"Searching report: {report}, starting at page ({start_page}/{num_pages})")
        candidates = [] # list of start page number and text content
        for i in range(start_page, num_pages):
            page = obj.getPage(i)
            text = page.extractText()
            if re.search(keyword, text, flags=re.IGNORECASE):
                if not secondary_keywords:
                    candidates.append((i, text))
                    continue
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
        if len(candidates) == 1:
            table_start_page = candidates[0][0]+1 # 0 index, so +1 for human
            print(f"    (only candidate) table start on Page: {table_start_page}")
        elif len(candidates) > 1:
            for page, text in candidates:
                if not tertiary_keywords:
                    table_start_page = page+1
                    print(f"   (filtered candidate) table start on Page: {table_start_page}")
                    break
                found = any(re.search(key, text, flags=re.IGNORECASE) for key in tertiary_keywords)
                if found:
                    table_start_page = page+1
                    print(f"   (filtered candidate) table start on Page: {table_start_page}")
                    break
        if not table_start_page:
            # Try again requiring all of secondary and tertiary keywords to be present
            for i in range(start_page, num_pages):
                page = obj.getPage(i)
                text = page.extractText()
                keys = secondary_keywords + tertiary_keywords
                secondary_tertiary_found = all(re.search(key, text, flags=re.IGNORECASE) for key in keys)
                if secondary_tertiary_found:
                    table_start_page = i+1
                    print(f"    (second chance) table start on Page: {table_start_page}")
                    break
            if not table_start_page:
                print(f'[WARNING] start page not found for {report}!!! {len(candidates)} candidates: {[c[0]+1 for c in candidates]}')
        code = re.search('_(\d+)\.pdf', report).group(1)
        result = {'code': code, 'pdf': report, 'table_start_page': table_start_page}
        results.append(result)
    return results


def detect_table_start():
    meta_df = pd.read_csv('data/pefa-assessments.csv', encoding='utf-8')
    meta_df_to_process = meta_df[(meta_df.Type == 'National') & (meta_df.Availability == 'Public') & (meta_df.Framework == '2016 Framework')]

    # takes a few minutes (<5min) to complete
    for index, row in meta_df_to_process.iterrows():
        download_pdf(row['Link to Content'], row['Language'], row['Country'])

    stage1_processed_pdfs = []
    for lang in meta_df_to_process.Language.unique():
        stage1_processed_pdfs += find_tables(lang)
    stage1_df = pd.DataFrame(stage1_processed_pdfs)
    stage1_df = stage1_df.astype({'table_start_page': 'Int64'})
    stage1_df['Link to Content'] = 'https://www.pefa.org/node/' + stage1_df.code
    stage1_df['table_last_page'] = ''
    stage1_df['comment'] = ''
    columns_ordered = ['code', 'pdf', 'Link to Content', 'table_start_page', 'table_last_page']
    stage1_df = stage1_df.reindex(columns=columns_ordered)
    stage1_df.to_csv('data/stage1.csv', index=False)

# detect_table_start()
stage1_df = pd.read_csv('data/stage1_reviewed.csv', encoding='utf-8')
stage1_df = stage1_df.astype({'table_start_page': 'Int64', 'table_last_page': 'Int64'})
for index, row in stage1_df.iterrows():
    pages = None
    if pd.notnull(row.table_start_page):
        pages = f'{row.table_start_page-1}-{row.table_last_page-1}'
    if not pages:
        print(f'No table in pdf {row.pdf}, skipping')
        continue
    tables = tabula.read_pdf(row.pdf, pages=pages)
    folder_name = f"data/csvs/{Path(row.pdf).stem}"
    Path(folder_name).mkdir(parents=True, exist_ok=True)
    for i, table in enumerate(tables):
        table.to_csv(f'{folder_name}/{i}.csv')
    # tables = camelot.read_pdf(row.pdf, pages=pages, backend="poppler")

