import os
import pandas as pd
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ES
import time,requests
from EmailFinding import find_email
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime

sess = requests.Session()

OUTPUT_FILE_PATH = "Data.csv"

header=False
if not os.path.exists(OUTPUT_FILE_PATH):
    f = open(OUTPUT_FILE_PATH,'w',encoding='UTF-8')
    f.write("\ufeff")
    f.close()
    header = True

def checkInternet():
    while True:
        try:
            sess.get('https://ipinfo.io',timeout=5)
            break
        except:
            print("Internet Issue...")
            time.sleep(1)
def captcha(driver):
    pageSource = str(driver.page_source)
    if pageSource.find(
            "Our systems have detected unusual traffic from your computer network.  This page checks to see if it's really you sending the requests, and") > -1:
        return True
    else:
        return False


def move_to_Frame(driver,element):
    driver.switch_to.default_content()
    driver.switch_to.frame(element)


def SOLVE_CAPTCHA(driver:webdriver.Chrome):
    main_iframe = driver.find_element(By.CSS_SELECTOR,'iframe[title="reCAPTCHA"]')
    second_iframe = driver.find_element(By.XPATH,'//iframe[@title="recaptcha challenge expires in two minutes"]')
    driver.switch_to.frame(main_iframe)
    driver.find_element(By.CSS_SELECTOR,'span#recaptcha-anchor').click()
    move_to_Frame(driver,second_iframe)
    WebDriverWait(driver,20).until(ES.visibility_of_element_located((By.CSS_SELECTOR,'div.button-holder.help-button-holder')))
    driver.find_element(By.CSS_SELECTOR,'div.button-holder.help-button-holder').click()
    move_to_Frame(driver,main_iframe)
    while True:
        try:
            WebDriverWait(driver,10).until(ES.presence_of_element_located((By.CSS_SELECTOR,'span[aria-checked="true"]')))
            break
        except:pass
        move_to_Frame(driver,second_iframe)
        driver.find_element(By.CSS_SELECTOR,'div.button-holder.help-button-holder').click()
        move_to_Frame(driver,main_iframe)
    print("[!] Captcha Solved Sucessfully")

def browser(proxy):

    options = webdriver.ChromeOptions()
    options.add_argument("--log-level=3")
    # options.add_argument("--headless")
    options.add_argument('--start-maximized')
    options.add_argument('--proxy-server=%s' % proxy)
    

    options.add_extension('ex.crx')
    driver = webdriver.Chrome(ChromeDriverManager().install(),chrome_options=options)
    return driver



def scraper(driver,key):
    global header
    if key == None: return False
    link = f"https://www.google.com/search?q={key[0]} in {key[1]}&hl=en&tbm=lcl"
    checkInternet()
    driver.get(link)
    if captcha(driver):
        checkInternet()
        SOLVE_CAPTCHA(driver)
    
    if driver.page_source.find('There are no local results matching your search')>-1:return False
    WebDriverWait(driver, 20).until(ES.visibility_of_element_located((By.XPATH, '//div[@jscontroller="AtSb"]')))
    

    Nameduplicates = []
    while True:
        cons = driver.find_elements(By.XPATH, '//div[@jscontroller="AtSb"]')
        for con in cons:
            checkInternet()
            while True:
                try:
                    con.click()
                    break
                except:
                    pass
            time.sleep(1.5)
            # name = con.find_element(By.XPATH,'//div[contains(@class, "dbg0pd") and contains(@class, "OSrXXb") and contains(@class, "eDIkBe")]').text
            # print(name)
            try:
                name = driver.find_element(By.XPATH,'//div[@class="SPZz6b"]/h2/span').text.strip()
                if name in Nameduplicates:
                    continue
                Nameduplicates.append(name)
            except:
                name = ''
            address = ''
            phone = ''
            Direction = ''
            datas = driver.find_elements(By.XPATH, '//div[contains(@class, "zloOqf") and contains(@class, "PZPZlf")]')
            
            try:
                rattings = con.find_element(By.CSS_SELECTOR,'span.yi40Hd.YrbPuc').text
            except:
                rattings = ''
            try:
                noOfR = con.find_element(By.CSS_SELECTOR,'span.RDApEe.YrbPuc').text.strip()
                noOfR = noOfR.removeprefix('(').removesuffix(')')
            except:
                noOfR = ''

            try:
                cate = driver.find_elements(By.CSS_SELECTOR,'span.YhemCb')[-1].text
                cate = cate.split(' in ')[0].strip()
            except:
                cate = ''
            try:
                Close = driver.find_element(By.XPATH, '//span[@id="Shyhc"]/span').text
            except:
                Close = "OPEN"
            

            for data in datas:
                try:
                    if data.text.find("Address") > -1:
                        address = data.text.replace("Address: ", '')
                        
                    if data.text.find("Phone") > -1:
                        phone = data.text.replace("Phone: ", '')
                        
                except:
                    pass
            try:
                website = [element.get_attribute('href') for element in driver.find_elements(By.CSS_SELECTOR,'a.dHS6jb') if element.text.lower().find('website')>-1][0]
                

            except: website=''
            try:
                maps_link = None
                for ind_url,a in enumerate(driver.find_elements(By.CSS_SELECTOR,'a[jsname="AznF2e"]')):
                    if a.text.strip() == "Menu":
                        a.click()
                        driver.implicitly_wait(0.5)
                        li = driver.find_elements(By.CSS_SELECTOR,'li[jsname="sRYx7b"]')[ind_url]
                        maps_link = li.find_element(By.TAG_NAME,'a').get_attribute('href')
                        break
            except: maps_link=None
            driver.implicitly_wait(0)
            try:
                imgsrc = driver.find_element(By.CSS_SELECTOR, 'a.llfsGb>div[role="img"]').get_attribute('style')
                imgsrc = imgsrc.removeprefix('background-image: url(\"').removesuffix("\");")
               
            except:
                imgsrc = ''


            temp = {
                "Category": key[0],
                "City": key[1],
                "Name": name,
                "Address": address,
                "Phone": phone,
                "Website": website,
                "Rattings": rattings,
                "Cate": cate,
                "Review_Count":noOfR,
                "Status": Close,
                "Map_Link": maps_link,
                "Img_Src": imgsrc,
                
            }
            print(f'[{datetime.strftime(datetime.now(),"%d-%m-%y %H:%M:%S")}]',name)
            pd.DataFrame([temp]).to_csv(OUTPUT_FILE_PATH,mode="a",index=False,header = header)
            header=False

        '''Saving Data to Sheet'''
        
        try:
            driver.find_element(By.XPATH, '//*[@id="pnnext"]').click()
        except NoSuchElementException:
            print("Element Not Found")
            input("")
            break
        except Exception as e:
            print("Some Error Occur... Retrying")
        time.sleep(3)

def main(k):
    driver = browser(proxy = "")
    scraper(driver,k)
    with open('Cachelog','a') as f:
        f.write(f"{k[0]} in {k[1]}\n")

cnks = lambda l,n: [l[i:i+n] for i in range(0,len(l),n)]

def load_cache():
    if os.path.exists('Cachelog'):
        with open('Cachelog') as f:
            tc = f.read().strip().splitlines()
    else:
        tc = []
    return tc

if __name__ == "__main__":
    cats = pd.read_csv("./input/Categories.csv").to_records()
    city = pd.read_csv("./input/Cities.csv").to_records()
    tc = load_cache()
    n =  int(input("[?] No of Threads?"))
    keywords = []
    for cat in cats: 
        for cty in city: 
            if f"{cat[1]} in {cty[1]}" not in tc:
                keywords.append([cat[1], cty[1]])
    for chunk in cnks(keywords,n+2):
        with ProcessPoolExecutor(max_workers=n,max_tasks_per_child=1) as P:
            for key in chunk:
                P.submit(main,key)

            P.shutdown(wait=True)      
        
   
