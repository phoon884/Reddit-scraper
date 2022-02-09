#lib imports
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json

#variable
classification = 'right'
subreddit = 'Republican'
html_parser = 'lxml'

request_delay = 1.5
delay_after_failed_req = 8

ignored_phrase = ['[deleted]','[removed]']
thread_category_list = ['text','video','image','external_source']

json_orientation = "records"
json_path =  None

length = 1000
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
}

columns=['title', 'upvotes', 'thread_category', 'thread_content' ,'comments', 'comment_list']
thread_df = pd.DataFrame(
    columns=columns)

reddit_video_domain = 'v.redd.it'
reddit_image_domain = 'i.redd.it'
text_domain = '/r/'+subreddit+'/comments/'

#helper function
def mk_req(href, headers=headers):
    while True:
        html = requests.get(href, headers=headers)
        if html.status_code >= 200 and html.status_code <= 299:
            time.sleep(request_delay)
            return html.text
        else:
            print("status code:", html.status_code)
            time.sleep(delay_after_failed_req)


def get_comments(html_text):
    soup = BeautifulSoup(html_text, html_parser)
    comments = soup.find_all('div', class_='entry unvoted')
    content = []
    for comment in comments:
        try:
            comment_sanitized = comment.find('div', class_='md').find('p').text
            if (comment_sanitized in ignored_phrase):
                continue
            content.append(comment_sanitized)
        except:
            continue
    return content

def get_text(html_text):
    text_sanitized = []
    soup = BeautifulSoup(html_text, html_parser)
    text = soup.find('div',attrs={'data-context': 'comments'})
    raw_text = text.find('div', class_='md').find_all('p')
    for p in raw_text:
        text_sanitized.append(p.text)
    return ' '.join(text_sanitized)

def get_thread_category(href):
    if text_domain in href:
        return thread_category_list[0]
    elif reddit_video_domain in href:
        return thread_category_list[1]
    elif reddit_image_domain in href:
        return thread_category_list[2]
    else:
        return thread_category_list[3]

# requests
try:
    html_text = mk_req('https://old.reddit.com/r/{}'.format(subreddit))


    while len(thread_df) < length:
        soup = BeautifulSoup(html_text, html_parser)
        thread_list = soup.find_all('div', attrs={"data-subreddit": subreddit})
        for thread in thread_list:
            if(len(thread_df) == length):
                break
            data = {}

            data[columns[0]] = thread.find('p', class_='title').find('a').text

            try:
                data[columns[1]] = int(thread.find(
                    'div', class_='score unvoted').text)
            except:
                data[columns[1]] = 0

            data_url = thread.get('data-url')
            data[columns[2]] = get_thread_category(data_url)

            try:
                data[columns[4]] = int(thread.find(
                    'a', attrs={"data-event-action": "comments"}).text.split(' ')[0])
            except:
                data[columns[4]] = 0
                
            comment_href = thread.find(
                'a', attrs={"data-event-action": "comments"}).get('href').replace(' ', '')
                
            if data[columns[2]] == thread_category_list[0] or data[columns[4]] != 0:
                html_comment_text = mk_req(comment_href)

            if data[columns[2]] == thread_category_list[0]:
                data[columns[3]] = get_text(html_comment_text)
            else: 
                data[columns[3]] = data_url

            if data[columns[4]] == 0:
                data[columns[5]] = []
            else:
                data[columns[5]] = get_comments(html_comment_text)
            thread_df = thread_df.append(data, ignore_index=True)

        try:
            next_href = soup.find(
                "span", class_="next-button").find('a').get('href')
        except:
            break
        print(next_href)
        html_text = mk_req(next_href)
except Exception as e:
    print(e)

result = thread_df.to_csv(path_or_buf=f"./{classification}/{subreddit}.csv")