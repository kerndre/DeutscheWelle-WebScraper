import requests
from bs4 import BeautifulSoup

import pandas as pd
import time
import re


def dw_webscraper(start_date:str, end_date:str):
    """
    dw_webscraper: The function dw_webscraper scrapes data from DW News website for a given time period and returns it in a Pandas DataFrame.

    Args:
        start_date (str): A string representing the start date of the period to be scraped in the format 'dd.mm.yyyy'.
        end_date (str): A string representing the end date of the period to be scraped in the format 'dd.mm.yyyy'.

    Returns:
        pd.DataFrame: A DataFrame with columns 'date', 'title', 'url', 'teaser', 'text', 'category', and 'region'. Each row of the DataFrame represents an article scraped from DW News website during the specified time period. The 'date' column contains the date of the article in the format 'yyyy-mm-dd'. The 'title' column contains the title of the article. The 'url' column contains the URL of the article. The 'teaser' column contains a short teaser text of the article. The 'text' column contains the full text of the article. The 'category' column contains the category of the article. The 'region' column contains the region affected by the article. If any information is missing for an article, it is represented by a 'None' value.

    The function uses several helper functions, scrape_title, scrape_url, scrape_text, scrape_category, and scrape_region, to extract the required information from the HTML content of the articles.
    """
    # support functions
    def scrape_title(article, count):
        # if, because every 10th article safes the title at a different position
        if count == 0 or count % 10 != 0:
            title = article.text.split('\n')[6]
        else: title = article.text.split('\n')[16]
        return title

    def scrape_url(article):
        # scraping the article web adress and completing it to the url
        tags = article.select('a')
        tag_hrefs = [tag.get('href') for tag in tags]
        url = 'https://www.dw.com' + tag_hrefs[0]
        return url

    def scrape_text(soup):
        # scraping the full article text
        try:
            text = soup.select_one('div.sc-gicCDI:nth-child(5)').text
        except AttributeError:
            return None
        # if article text is shorter than 120 characters (no real article) return None
        if len(text) < 120:
            return None
        return text

    def scrape_category(soup):
        # scraping article category
        try:
            category = soup.select_one('div.sc-kLLXSd:nth-child(1) > span:nth-child(1)').text
        except AttributeError:
            return None
        return category

    def scrape_region(soup):
        # scraping article affected region
        try:
            region = soup.select_one('div.sc-kLLXSd:nth-child(1) > span:nth-child(2)').text
        except AttributeError:
            return None
        return region
    
    # creating frame for resulting data frame
    df = pd.DataFrame(columns=['date','title', 'url', 'teaser', 'text', 'category', 'region'])

    # scraping only for the total number of articles in the time span
    url = f'https://www.dw.com/search/?languageCode=en&searchNavigationId=9097-30688-8120&from={start_date}&to={end_date}&sort=DATE&resultsCounter=10'
    html = requests.get(url)
    soup = BeautifulSoup(html.content, 'html.parser') 
    # total number of articles as int  
    n_articles = soup.select_one('span.hits.from').text
    int_articles = int(n_articles)
        
    # getting the whole list of articles in the time span for further scraping
    url = f'https://www.dw.com/search/?languageCode=en&searchNavigationId=9097-30688-8120&from={start_date}&to={end_date}&sort=DATE&resultsCounter={int_articles}'
    html = requests.get(url)
    soup = BeautifulSoup(html.content, 'html.parser')   

    # searching for all articles
    articles = soup.find_all('div', attrs={'class':'searchResult'})

    # counter for the for-loop
    counter = 0
        
    # scraping for each article from all articles
    for article in articles:
        # scraping date, title and short text
        date = article.select_one('span.date').text
        title = scrape_title(article, counter)
        teaser = article.find('p').text

        # following scraping with url of single article
        url = scrape_url(article)
        html = requests.get(url)
        soup = BeautifulSoup(html.content, 'html.parser')

        # scraping for article text, category and affected region
        text = scrape_text(soup)
        category = scrape_category(soup)
        region = scrape_region(soup)
            
        # bringing all sraped information into df
        new_row = pd.DataFrame({'date':[date], 'title':[title], 'url':[url], 'teaser':[teaser], 'text':[text], 'category':[category], 'region':[region]})
        df = pd.concat([df, new_row], ignore_index=True)

        # counter +1
        counter += 1
        # timer for 1 sec. pause for polite scaping
        time.sleep(1)
    
    # raises error if dates are in wrong format -> the df is empty
    if df.shape[0] == 0:
        raise Exception("date must be specified in the format 'dd.mm.yyyy'")
    
    return df