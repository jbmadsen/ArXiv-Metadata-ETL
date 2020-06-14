import re 
import pandas as pd
import requests
from bs4 import BeautifulSoup, NavigableString, Tag


def find_table_after_heading(soup, heading_text):
    """Function for finding tables after specific headers text in BeautifulSoup

    Args:
        soup (bs4 Soup object): The instance of BeatutifulSoup containing requests content
        heading_text (string): The heading text immediately before the table

    Returns:
        bs4 Object: Object containing table data
    """
    # Stolen from: https://stackoverflow.com/a/51936486
    for header in soup.find_all('h3', text=re.compile(heading_text)):
        nextNode = header
        while True:
            nextNode = nextNode.nextSibling
            if nextNode is None:
                break
            if isinstance(nextNode, Tag):
                if nextNode.name == "h3":
                    break
                #print(nextNode)
                return nextNode
    return None


def download_data():
    """
    Scraping and downloading Subject Classifications data table from from arxiv.org
    
    Dependant libraries: requests, pandas, bs4
    """

    # Configurations
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"}
    page = 'https://arxiv.org/help/api/user-manual'

    print("Downloading classifications")
    print("Creating requests session")

    with requests.Session() as s:
        # Get data
        r = s.get(page, headers = headers)
        soup = BeautifulSoup(r.content, 'lxml')
        # Find table data we are looking for
        table_node = find_table_after_heading(soup, '5.3. Subject Classifications')
    
    # Iterate over table data and convert dataframe
    data = []
    cols = ['tag','name']
    for tr in table_node.find_all('tr')[2:]:
        tds = tr.find_all('td')
        data.append([tds[0].text, tds[1].text])
        #print(f"tag: {tds[0].text}, name: {tds[1].text}")
    
    print("Loading data from source")
    df = pd.DataFrame.from_records(data, columns=cols)

    # Save dataframe to disk
    print("Saving data to disk")
    df.to_csv('./data/subject-classifications.csv', index=False)

    print("Done")


if __name__ == "__main__":
    download_data()