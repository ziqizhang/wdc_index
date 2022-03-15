'''
Utility code to batch download content from WDC
'''
import urllib.request, os, shutil, requests, sys
from bs4 import BeautifulSoup

'''
Download all zipped table data from 
http://webdatacommons.org/structureddata/schemaorgtables/
(see at the bottom of the page)
'''
def download_schemaorgtables(in_webpage_url, outfolder,
                             top100=True, mininum3=False, rest=False):

    fp = urllib.request.urlopen(in_webpage_url)
    data = fp.read()
    html = data.decode("utf8")
    soup = BeautifulSoup(html)
    links=soup.find_all("a", href=True)

    for l in links:
        l = l['href']
        if not l.endswith(".zip"):
            continue
        if ('_top100' in l and top100) or ('_minimum3' in l and mininum3) or ('_rest' in l and rest):
            print(l)
            local_filename = l.split('/')[-1]
            path = os.path.join("/{}/{}".format(outfolder, local_filename))
            with requests.get(l, stream=True) as r:
                with open(path, 'wb') as f:
                    shutil.copyfileobj(r.raw, f)

    fp.close()

def scrape_schemaorgtable_links(in_webpage_url, outfile,
                             top100=False, mininum3=True, rest=False):
    f = open(outfile, "a")

    fp = urllib.request.urlopen(in_webpage_url)
    data = fp.read()
    html = data.decode("utf8")
    soup = BeautifulSoup(html)
    links=soup.find_all("a", href=True)

    for l in links:
        l = l['href']
        if not l.endswith(".zip"):
            continue
        if ('_top100' in l and top100) or ('_minimum3' in l and mininum3) or ('_rest' in l and rest):
            f.write(l+"\n")
    f.close()

    fp.close()

if __name__=="__main__":
    # download_schemaorgtables(sys.argv[1],
    #                          sys.argv[2])
    scrape_schemaorgtable_links(sys.argv[1],
                             sys.argv[2])