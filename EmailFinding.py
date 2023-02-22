from bs4 import BeautifulSoup
import re,requests
from concurrent.futures import ThreadPoolExecutor
import urllib3,sys,os
import pandas as pd
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class find_email():
    def __init__(self) -> None:
        # self.client = httpx.Client(headers={"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"})
        self.client = requests.Session()
        self.client.headers.update({"user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36"})
        self.phone_regex = r"\d{3}[-\.\s]\d{3}[-\.\s]\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]\d{4}|\d{5}[-\.\s]\d{4}[-\.\s]\d{4}|\d{3}[-\.\s]\d{3}[-\.\s]\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]\d{4}|\d{3}[-\.\s]\d{4}[-\.\s]\d{4}|\d{3}[-\.\s]\d{3}[-\.\s]\d{4}|\(\d{3}\)\s*\d{3}[-\.\s]\d{4}|\d{4}[-\.\s]\d{4}[-\.\s]\d{4}|[\+]\d{10,30}"
        self.email_regex = r"[a-zA-Z0-9._-]+@[a-zA-Z0-9-]+\.+[a-zA-Z0-9\.\-\_]+"
        self.fb_regex = r"facebook.com/[\w]+"
        self.in_regex= r"linkedin.com/in/[\w]+|it.linkedin.com/in/[\w]+|linkedin.com/company/[\w]+|it.linkedin.com/company/[\w]+"
    def get_website(self,url):
        url = self.get_domain(url)
        return "http://{}".format(url)
    def run(self,website):
        self.total_emails = []
        self.total_phones = []
        self.fb = []
        self.linked = []
        website = self.get_website(website)
        links = self.find_all_pages(website)
        self.get_pho_emails(links)
        return self.total_emails,self.total_phones
    def get_domain(self, website):
        url = str(website).lower().removeprefix('http://')
        url = url.removeprefix('https://')
        url = url.removeprefix('www.')
        domain = url.split('/')[0]
        return domain
    
    def get_response(self,url):
        no_retryies = 10
        if str(url) =='http://nan':return None
        while True:
            try:
                res = self.client.get(url,verify=False,allow_redirects=True)
                break
            except requests.exceptions.InvalidURL:return None
            except requests.exceptions.InvalidSchema: return None
            except Exception as e:
                if no_retryies == 0:return None
                no_retryies -=1
                
                print('Retrying for the reason {}'.format(e))
        return res
    def find_all_pages(self, website):
        res = self.get_response(website)
        if None == res:
            return []
        soup = BeautifulSoup(res.text,'lxml')
        a_tags = soup.findAll('a')
        all_links = []
        for a_tag in a_tags:
            try:
                href = str(a_tag['href'])
                if href.find('http')==-1:
                    if website+href not in all_links:
                        all_links.append(website+href)

                if href.find(self.get_domain(website))>-1:
                    if href not in all_links:
                        all_links.append(href)
            except Exception as e: pass #print(e)
        return all_links
        
    def get_regex(self,regex,text):
        instances = re.findall(regex,text)
                   
        return instances
    def multi_thred(self,link):
        try:
            # print(link)
            res = self.client.get(link,timeout=10,allow_redirects=True)
            for email in self.get_regex(self.email_regex,res.text):
                if email not in self.total_emails and email.find('sentry')==-1 and email.find('@2x')==-1 and email.find('.png')==-1 and email.find('.jpg')==-1 and email.find('example.com')==-1 and email.find('.js')==-1:
                    self.total_emails.append(email)
            for phone in self.get_regex(self.phone_regex,res.text):
                if phone not in self.total_phones:
                    self.total_phones.append(phone)
            
            for fb in self.get_regex(self.fb_regex,res.text):
                if fb not in self.fb:
                    self.fb.append(fb)
            for linked in self.get_regex(self.in_regex,res.text):
                if linked not in self.linked:
                    self.linked.append(linked)
        except Exception as e: pass # print(e)
    def get_pho_emails(self,all_links):
        with ThreadPoolExecutor(max_workers=100) as ex:
            {ex.submit(self.multi_thred,link) for link in all_links}
            ex.shutdown(wait=True)
            
def main(url):
    lnkd = [None]
    g_emails = [None]*5
    fb = None
    g_phoes = [None]

    obj = find_email()
    res = obj.run(url)
    g_emails = res[0][:5]
    while True:
        if len(g_emails)>=5:
            break
        else:g_emails.append('')
    if len(res[1]):
        g_phoes = res[1][:1]
    if len(obj.fb): fb = obj.fb[0]
    if len(obj.linked): lnkd = obj.linked[0]
    print("[Done]",url)
    if lnkd[0] == None: lnkd = None
    # return {
    # "URL": url,
    # "Linkedin":lnkd[0],
    # "Email1":g_emails[0],
    # "Email2":g_emails[1],
    # "Email3":g_emails[2],
    # "Email4":g_emails[3],
    # "Email5":g_emails[4],
    # "Mobile":g_phoes[0],
    # "Facebook": fb,}
    return [url,lnkd,g_emails[0],g_emails[1],g_emails[2],g_emails[3],g_emails[4],g_phoes[0],fb]
cnks = lambda l,n: [l[i:i+n] for i in range(0,len(l),n)]

def read_csv(filepath)->pd.DataFrame:
    if os.path.exists(filepath):
        recs = pd.read_csv(filepath,encoding='utf-8',encoding_errors='ignore')
    else:
        sys.exit("File Not Found....")
    return recs



if __name__ == "__main__":
    file_path = input("[?] Input Business data File Path.(Default: Data.csv)? ")
    if file_path == '': file_path = 'Data.csv'
    df = read_csv(file_path)
    websites = list(df['Website'])
    n = int(input("[?] Input no of Threads?"))
    for chunk in cnks(websites,n):
        with ThreadPoolExecutor(max_workers=n) as p:
            results = [p.submit(main,url) for url in chunk]
            temp = []
            # results = [result.result() for result in results]
            # pd.DataFrame(results).to_csv("Emails_data.csv",index=False,header=not os.path.exists("Emails_data.csv"),mode='a',encoding='utf-8',errors='ignore')
            for result in results:
                res = result.result()
                index = df.index[df['Website']==res[0]]
                df.loc[index, ["Linkedin","Email1","Email2","Email3","Email4","Email5","Mobile","Facebook"]] = res[1:]
            df.to_csv('NewData.csv',index=False,encoding='UTF-8',errors='ignore')
