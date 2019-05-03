import requests
import pandas as pd
import time
import getpass
import time
from tqdm import tqdm
from tqdm import tqdm_notebook as tqdm
import emailer
import numpy as np
try:
    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    import selenium.webdriver.support.ui as sui
    from bs4 import BeautifulSoup
except:
    raise Exception("Please check that Selenium and BeautifulSoup is installed")

def automatic_save(flag, item, counter, filename="filename", max_count=100, email_me=False):
    if flag:
        if counter % max_count == 0:
            if email_me:
                emailer.send_email("saved!", "everything is good!")
            try:
                pd.DataFrame(item).to_json(f'{filename}.json')
            except:
                 emailer.send_email("Error!", "file was unabled to be saved!")
            finally:
                print(counter)

def clean_text(lst, regular=False):
    if regular:
        text_lst = [(" ").join(item[item.index("\n")+1:].split("\n")) for item in lst]
    else:
        text_lst = [item.split("\n") for item in lst]
    return text_lst
        
class ApiCaller(object):
    # base class for gathering data from yelp using its api
    
    def __init__(self, url):
        self.url = url
        self._api_key = None
        self._client_id = None
        try:
            import config
            self._api_key = config.api_key
            self._client_id = config.client_id
        except:
            self._api_key = getpass.getpass("Enter your Yelp API key:")
            self._client_id = getpass.getpass("Enter your Yelp client id:")
        
    @property
    def key(self):
        return self._api_key
    
    @key.setter
    def key(self, key_value):
        self._api_key = key_value
        
    @key.deleter
    def key(self):
        self._api_key = None
    
    @property
    def client_id(self):
        return self._client_id
    
    @client_id.setter
    def client_id(self, id_value):
        self._client_id = id_value
        
    @client_id.deleter
    def client_id(self):
        self._client_id = None
        
    def gather_data(self, search_term, location, limit=50):    
        term = search_term
        location = location
        SEARCH_LIMIT = limit
        sort_by = "review_count"
        offset = 0
        search_count = SEARCH_LIMIT
        data = []

        url = self.url

        headers = {
                'Authorization': 'Bearer {}'.format(self.key),
            }
        count = 0
        while search_count >= 0:
            url_params = {
                            'term': term.replace(' ', '+'),
                            'location': location.replace(' ', '+'),
                            'sort_by':sort_by,
                            'limit': SEARCH_LIMIT,
                            'offset':offset
                        }
            yelp_api = requests.get(url, headers=headers, params=url_params)
            data.append(yelp_api.json())
            offset += SEARCH_LIMIT
            search_count = data[0]["total"] - offset
            count += 1
            if count % 100 == 0:
                print(count)
        return data
    
    def gather_single_loc_data(self, terms, location):
        data_lst = []
        for term in terms:
            data_lst.extend(self.gather_data(term, location))
            time.sleep(1)
        return data_lst
    
    def gather_many_loc_data(self, terms, locations):
        data_lst = []
        for location in locations:
            data_lst += (self.gather_single_loc_data(terms, location))
        return data_lst
            
    
class TutoringDataGrabber(ApiCaller):
        
    def __init__(self, locations):
        ApiCaller.__init__(self, 'https://api.yelp.com/v3/businesses/search')
        self._terms = ["test prep", "after school", "tutoring", "learning center", "math", "ela", "homework help"]
        self._locations = locations
        self._ending_index = len(self._locations)
       
    @property
    def terms(self):
        return self._terms
    
    @terms.setter
    def terms(self, term):
        if term not in self.terms:
            self._terms.append(term)
        else:
            raise Exception("New item is already in the list!")
        
    @property
    def locations(self):
        return self._locations
    
    @locations.setter
    def locations(self, place):
        if place not in self.locations and type(place)==str:
            self._locations.append(place)
        elif place in self.locations:
            raise Exception("New item is already in the list!")
        else:
            raise Exception("Incorrect data type. Type must be a str!")
        
    def del_term(self, term):
        self._terms.remove(term)
        
    def gather_tutoring_data(self):
        data = []
        data += self.gather_many_loc_data(self.terms, self.locations)
        return data
    
    
class YelpScraper(object):
    
    def __init__(self, driver_location):
        self._driver_location = driver_location
            
    def get_specific_data(self, driver, class_name, att="title", get_info="class"):
        elems = driver.find_elements_by_class_name(class_name)
        final = []
        if get_info == "att":
            for elem in elems:
                info = elem.get_attribute(att)
                final.append(info)
        else:
            for elem in elems:
                info = elem.text
                final.append(info)
        return final
    
    def get_meta_data(self, driver, subj):
        page = driver.page_source
        soup = BeautifulSoup(page, 'html.parser')
        meta = soup.findAll("meta")
        data = []
        for item in str(meta).split("<meta content="):
            if subj in item:
                try:
                    data.append(item.strip(f' itemprop="{subj}"/>, '))
                except:
                    continue
        return data
    
    def get_certain_info(self, df, col, new_key, class_name, save_file=False, filename=None, max_count=100):
        driver = webdriver.Chrome(self._driver_location)
        lst = []
        for idx, row in tqdm(df.iterrows(), total=df.shape[0]):
            idx+=1
            driver.get(row[col])
            lst.append({row[new_key]: self.get_specific_data(driver, class_name)})
            automatic_save(save_file, lst, idx, filename, max_count)
            time.sleep(1)
        driver.close()
        return lst

class TutoringScraper(YelpScraper):
    
    def __init__(self, driver_location):
        YelpScraper.__init__(self, driver_location)
       
    def get_star_ratings(self, driver):
        return self.get_meta_data(driver, "ratingValue")
    
    def get_user_name(self, driver):
        return self.get_meta_data(driver, "author")
    
    def get_date_published(self, driver):
        return self.get_meta_data(driver, "datePublished")
    
    def get_user_reviews(self, driver):
        return self.get_specific_data(driver, "review-content")
    
    def get_user_passport(self, driver):
        return self.get_specific_data(driver, "user-passport-stats")
        
    def get_user_info(self, driver):
        return self.get_specific_data(driver, "user-passport-info")
    
    def get_reviews(self, driver): 
        user_info = self.get_user_info(driver)
        user_passport = self.get_user_passport(driver)
        review_date = self.get_date_published(driver)
        user_review = self.get_user_reviews(driver)
        user_star = np.array(self.get_star_ratings(driver)[1:])
        return user_info, user_passport, review_date, user_review, user_star
    
    def get_all_reviews(self, df, save_file=True):
        driver = webdriver.Chrome(self._driver_location)
        runs = 0
        size = df.shape[0]
        business_list = []
        for num in tqdm(range(df.url.shape[0])):
            business = {}
            try:
                count = 0
                runs += 1
                link = df.url[num]
                url = link[:link.index("?")]
                driver.get(url) 
                biz_name = df.name[num]
                business[biz_name] = {"User": [], "User_Stat" : [], 
                                          "Rev_Date" :[], "User_Rev" : [], 
                                          "Star_Rating" : []}
                try:
                    pages = int(self.get_specific_data(driver, "page-of-pages")[0][-1])
                    while count <= pages*20:
                            driver.get(url+f"?start={count}") 
                            uname, upass, ud, urevs, us = self.get_reviews(driver)
                            ur = clean_text(urevs, regular=True)
                            un = clean_text(uname)
                            up = clean_text(upass)
                            business[biz_name]["User"].extend(un)
                            business[biz_name]["User_Stat"].extend(up)
                            business[biz_name]["Rev_Date"].extend(ud)
                            business[biz_name]["Star_Rating"].extend(us)
                            business[biz_name]["User_Rev"].extend(ur)
                            count += 20
                    automatic_save(save_file, business, runs, filename="scraped_rev", max_count=150)        
                    if runs in [int(size/2), int(size/4), int((3*size)/4)]:
                        emailer.send_email("Still Running", f"Still Going: {(float(runs)/float(size))*100}% through.") 
                    time.sleep(1)
                except Exception as e:
                    emailer.send_email("Error 1!", str(e) + f" Error for {biz_name} at {runs} runs.")
                    continue

            except Exception as e:
                pd.DataFrame(business).to_json("scraped_reviews.json")
                emailer.send_email("Error 2!", e)
                return business_list
            business_list.append(business)

        emailer.send_email("Done!", "Done")           
        driver.close()
        return business_list

    def get_website(self, df, save_file=False):
        return self.get_certain_info(df, "url", "name", "biz-website", save_file)

    def get_hours(self, df, save_file=False):
        return self.get_certain_info(df, "url", "name", "hours-table", save_file)
        
        
