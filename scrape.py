#  1 = Scraping ###############################################################################################################################

##### Obecné ############
import pandas as pd  # for dataframes' manipulation
import time  # Time measuring
import re
import requests
import json  # for Requests
from consts import category_main_cb, category_type_cb, category_sub_cb


# 1 = Scraping ################################################################################################################################

def get_soup_elements(typ_stavby="byty", total_pages=None):
    """

    :param typ_stavby: byty or domy
    :param total_pages: Page count to scrape
    :return:
    """
    categories = {
        "byty": 1,
        "domy": 2
    }
    category = categories[typ_stavby]
    page = 1
    ids = dict()
    while True:
        print(f"page: {page}\t")
        if total_pages is not None and page > total_pages:
            return ids
        url = f"https://www.sreality.cz/api/cs/v2/estates?category_main_cb={category}&category_type_cb=1&per_page=100&page={page}"
        response = requests.get(url)
        if response.status_code != 200:
            break
        response_json = response.json()
        for estate in response_json['_embedded']['estates']:
            seo = estate['seo']
            hash_id = estate['hash_id']
            # https://www.sreality.cz/detail/prodej/dum/rodinny/karvina-lazne-darkov-lazenska/3917620300

            estate_url = f"https://www.sreality.cz/detail/"
            estate_url += f"{category_type_cb[seo['category_type_cb']]}/"
            estate_url += f"{category_main_cb[seo['category_main_cb']]}/"
            estate_url += f"{category_sub_cb[seo['category_sub_cb']]}/"
            estate_url += f"{seo['locality']}/"
            estate_url += f"{hash_id}"

            ids[hash_id] = estate_url
        page += 1
    return ids


#  2 = Získání URLS ###############################################################################################################################

def elements_and_ids(x):
    elements = pd.DataFrame({"url": x})

    def get_id(x):
        x = x.split("/")[-1]
        return x

    elements["url_id"] = elements["url"].apply(get_id)

    len1 = len(elements)
    # Přidáno nově, v tuto chvíli odmažu duplikáty a jsem v pohodě a šetřím si čas dál.
    elements = elements.drop_duplicates(subset=["url", "url_id"], keep="first", inplace=False)
    len2 = len(elements)

    print("-- Vymazáno " + str(len1 - len2) + " záznamů kvůli duplikaci.")
    return elements


# 3 = získání Souřadnic, Ceny a Popisu = z JSON ################################################################################################################################

def parse_data(url, url_id):
    res = {'url': url, 'url_id': url_id}
    url = "https://www.sreality.cz/api/cs/v2/estates/" + str(url_id)

    import requests

    cookies = {
        'qusnyQusny': '1',
        '__cc': 'NHFZYndMdnBEU0RtV3Y0cjsxNjgwMjc0NjY0:SDdrZ1lvb3o0TG13VlJGZzsxNjgwMjg5MDY0',
        'lps': 'eyJfZnJlc2giOmZhbHNlLCJfcGVybWFuZW50Ijp0cnVlfQ.ZCbW8Q.M_8wv9Q3LRCS3Wc9HwLXCDhBrkM',
        '.seznam.cz|sid': 'id=14590615296785149072|t=1680247905.212|te=1680266993.590|c=79E56B1F2EA512D22890012327312E5F',
        'sid': 'id=14590615296785149072|t=1680247905.212|te=1680266993.590|c=79E56B1F2EA512D22890012327312E5F',
        'sid': 'id=14590615296785149072|t=1680247905.212|te=1680266993.590|c=79E56B1F2EA512D22890012327312E5F',
        'euconsent-v2': 'CPeUIIAPeUIIAD3ACBCSC-CsAP_AAEPAAATIIXIBhCokBSFCAGpYIIsAAAAHxxAAYCACABAAgAABABIAIAQAAAAQAAQgBAACABQAIAIAAAAACEBAAAAAAAAAAQAAAAAAAAAAIQAAAAAAAiBAAAAAAABAAAAAAABAQAAAgAAAAAIAQBAAAAEAgAAAAAAAAAAAAAAAAQgAAAAAAAAAAAgAAAAAEELoFQACwAKgAXAAyACAAGQANAAcwBEAEUAJgATwAqgBiAD8AISARABEgCOAE4AKUAWIAywBmgDuAH6AQgAiwBJgC0AF1AMCAawA2gB8gE2gLUAW4AvMBkgDLgGlANTAhcAA.YAAAAAAAAAAA',
        'szncmpone': '1',
        'seznam.cz|szncmpone': '1',
        'szncsr': '1680266994',
        '__gfp_64b': 'px1hPlNEDDQQNTW.DHcmHNR9uGPg1C9e1RN1BSu1QZ3.J7|1680266994',
        'pubmatic.com|SyncRTB3': '1682812800%3A203%7C1680825600%3A223_15%7C1681516800%3A35%7C1681084800%3A63%7C1681430400%3A55_56_161_13_7_54_3_233_220_21_71_166_8_22_251',
        'pubmatic.com|pi': '49307:2',
        'pubmatic.com|chkChromeAb67Sec': '1',
        'pubmatic.com|KADUSERCOOKIE': '3564C814-DCAD-45C0-A023-29E6C8DC0086',
        'pubmatic.com|DPSync3': '1681430400%3A241_235_201_245',
        'seznam.cz|KADUSERCOOKIE': '3564C814-DCAD-45C0-A023-29E6C8DC0086',
        'udid': 'IDvV_LgQNc3BizuWd-Vc2txQkp8o7mrC@1680267002298@1680267002298',
        'cmprefreshcount': '0|6km9fqu0mu',
    }

    headers = {
        'authority': 'www.sreality.cz',
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-CZ,en;q=0.9,de-CZ;q=0.8,de;q=0.7,en-GB;q=0.6,en-US;q=0.5,cs;q=0.4',
        'cache-control': 'no-cache',
        # 'cookie': 'qusnyQusny=1; __cc=NHFZYndMdnBEU0RtV3Y0cjsxNjgwMjc0NjY0:SDdrZ1lvb3o0TG13VlJGZzsxNjgwMjg5MDY0; lps=eyJfZnJlc2giOmZhbHNlLCJfcGVybWFuZW50Ijp0cnVlfQ.ZCbW8Q.M_8wv9Q3LRCS3Wc9HwLXCDhBrkM; .seznam.cz|sid=id=14590615296785149072|t=1680247905.212|te=1680266993.590|c=79E56B1F2EA512D22890012327312E5F; sid=id=14590615296785149072|t=1680247905.212|te=1680266993.590|c=79E56B1F2EA512D22890012327312E5F; sid=id=14590615296785149072|t=1680247905.212|te=1680266993.590|c=79E56B1F2EA512D22890012327312E5F; euconsent-v2=CPeUIIAPeUIIAD3ACBCSC-CsAP_AAEPAAATIIXIBhCokBSFCAGpYIIsAAAAHxxAAYCACABAAgAABABIAIAQAAAAQAAQgBAACABQAIAIAAAAACEBAAAAAAAAAAQAAAAAAAAAAIQAAAAAAAiBAAAAAAABAAAAAAABAQAAAgAAAAAIAQBAAAAEAgAAAAAAAAAAAAAAAAQgAAAAAAAAAAAgAAAAAEELoFQACwAKgAXAAyACAAGQANAAcwBEAEUAJgATwAqgBiAD8AISARABEgCOAE4AKUAWIAywBmgDuAH6AQgAiwBJgC0AF1AMCAawA2gB8gE2gLUAW4AvMBkgDLgGlANTAhcAA.YAAAAAAAAAAA; szncmpone=1; seznam.cz|szncmpone=1; szncsr=1680266994; __gfp_64b=px1hPlNEDDQQNTW.DHcmHNR9uGPg1C9e1RN1BSu1QZ3.J7|1680266994; pubmatic.com|SyncRTB3=1682812800%3A203%7C1680825600%3A223_15%7C1681516800%3A35%7C1681084800%3A63%7C1681430400%3A55_56_161_13_7_54_3_233_220_21_71_166_8_22_251; pubmatic.com|pi=49307:2; pubmatic.com|chkChromeAb67Sec=1; pubmatic.com|KADUSERCOOKIE=3564C814-DCAD-45C0-A023-29E6C8DC0086; pubmatic.com|DPSync3=1681430400%3A241_235_201_245; seznam.cz|KADUSERCOOKIE=3564C814-DCAD-45C0-A023-29E6C8DC0086; udid=IDvV_LgQNc3BizuWd-Vc2txQkp8o7mrC@1680267002298@1680267002298; cmprefreshcount=0|6km9fqu0mu',
        'dnt': '1',
        'pragma': 'no-cache',
        'referer': url,
        'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    }

    params = {
        'tms': str(time.time()),
    }

    response = requests.get(url, params=params, cookies=cookies,
                            headers=headers)
    byt = json.loads(response.text)
    try:
        coords = (byt["map"]["lat"], byt["map"]["lon"])
    except:
        coords = (0.01, 0.01)  # Pro případ neexisutjících souřadnic
    try:
        price = byt["price_czk"]["value_raw"]
    except:
        price = -1

    try:
        description = byt["meta_description"]
    except:
        description = -1

    try:
        locality = byt["locality"]['value']
    except:
        locality = ""

    try:
        text = byt['text']['value']
    except:
        text = ""
    items = []
    try:
        items = byt["items"]
    except:
        ...

    wanted_types = {"string": lambda x: (x['name'], x['value']),
                    "area": lambda x: (x['name'], x['value']),
                    "boolean": lambda x: (x['name'], x['value']),
                    'energy_efficiency_rating': lambda x: (x['name'], x['value_type']),
                    'set': lambda x: (x['name'], ','.join([v['value'] for v in x['value']]))
                    }
    for item in items:
        try:
            if item['type'] in wanted_types:
                key, val = wanted_types[item['type']](item)
                res[key] = val
        except:
            ...

    res['coords'] = coords
    res['cena_zobrazovana'] = price
    res['cena_databazova'] = int(round(price / 1.0953))
    res['popis'] = description
    res['locality'] = locality
    res['text'] = text
    return res


# Severní Šířka = latitude
# výchoDní / zápaDní Délka = longitude

def latitude(x):  # Rozdělí souřadnice na LAT a LON
    x = str(x).split()[0][1:8]
    return x


def longitude(x):
    x = str(x).split()[1][0:7]
    return x


#  4 = Prodej + Dům + Pokoje = z URL ###############################################################################################################################

def characteristics(x):
    x = x.split("/")
    buy_rent = x[2]
    home_house = x[3]
    rooms = x[4]

    return buy_rent, home_house, rooms


#  5 =  Plocha z Popisu ###############################################################################################################################

# Upraveno pro čísla větší než 1000 aby je to vzalo
# Zároveň se to vyhne velikost "Dispozice", "Atpyický", atd.

def plocha(x):
    try:
        metry = re.search(r'\s[12]\s\d{3}\s[m]', x)[
            0]  # SPecificky popsáno: Začíná to mezerou, pak 1 nebo 2, pak mezera, pak tři čísla, mezera a pak "m"
        metry = metry.split()[0] + metry.split()[1]  # Separuju Jedničku + stovky metrů, bez "m"
    except:
        try:
            metry = re.search(r'\s\d{2,3}\s[m]', x)[0]  # Mezera, pak 1-3 čísla, mezera a metr
            metry = metry.split()[0]  # Separuju čísla, bez "m"
        except:
            metry = -1
    return metry


# 6 = Adresy z předešlých inzerátů a short_coords ###############################################################################################################################

# Vytvoření ořezaných souřadnic, přesnost je dostatečná, lépe se najdou duplikáty
def short_coords(x):
    """
    x = x.astype(str)   # Bylo potřeba udělat string - ale Tuple se blbě převádí - vyřešil jsem uložením a načtením skrz excel
    """

    x1 = re.split(r'\W+', x)[1] + "." + re.split(r'\W+', x)[2]
    x1 = round(float(x1), 4)

    x2 = re.split(r'\W+', x)[3] + "." + re.split(r'\W+', x)[4]
    x2 = round(float(x2), 4)

    return (x1, x2)


#############################

# Napmapuje až 80 % Adres z předešlých inzerátů
def adress_old(x):
    adresy = pd.read_excel("data/Adresy.xlsx")
    adresy = adresy[["oblast", "město", "okres", "kraj", "url_id", "short_coords"]]

    # Nejlepší napárování je toto:
    # alternativně Inner a Left minus řádky s NaNs a funguje stejně)

    x.short_coords = x.short_coords.astype(
        str)  # získat string na souřadnice, protože v Načteném adresáři je mám už taky jako string
    data = pd.merge(x, adresy, on=["short_coords", "url_id"],
                    how="left")  # upraveno matchování na url_ID + short_coords, je to tak iv Adresáří, je to jednoznačné, jsou tam unikátní.
    # Pokud si v dalším kroku dostáhnu ke starému url_id a k nové coords ještě novou adresu, tak pak se mi uloží do Adresáře nová kombiance ID + short_coord a je to OK
    # Viz funkce"update_databáze_adres() kde je totéž info

    print("-- Počet doplněných řádků je: " + str(len(data[~data.kraj.isna()])) + ", počet chybějících řádků je: " + str(
        len(data[data.kraj.isna()])))

    return data


#################################################################


def ownership_share(description: str):
    description = description.lower()
    if "podíl" not in description and "podil" not in description:
        return

    sentences = description.split('. ')
    for sentence in sentences:
        if "podíl" not in sentence and "podil" not in sentence:
            continue
        for m in re.finditer("( |$)(\d+)/(\d+)( |$)", sentence):
            try:
                _, num1, num2, _ = m.groups()
                num1 = int(num1)
                num2 = int(num2)
                if num1 / num2 <= 1:
                    return num1 / num2
            except ValueError:
                ...

    # Parametry:


# "prodej"/ "pronájem"
# "byty"/"domy"
# pages = 1- X, případně 999 = Všechny strany !

def scrap_all(typ_stavby="byty", pages=None):
    # Scrapni data - hezky komunikuje = cca 50 min
    urls = get_soup_elements(typ_stavby=typ_stavby, total_pages=pages)
    print("1/8 Data scrapnuta, získávám URLs.")

    # 3 = získání Souřadnic, Ceny a Popisu = z JSON
    out = [parse_data(val, key) for key, val in urls.items()]
    data = pd.DataFrame(out)
    data["lat"] = data["coords"].apply(latitude)
    data["lon"] = data["coords"].apply(longitude)
    data.to_excel(r"a2_Souřadnice_prodej_byty.xlsx")
    print("3/8 Získány Souřadnice, Ceny a Popis, nyní získávám informace z URLs.")

    # 4 = Prodej + Dům + Pokoje = z URL
    data["prodej"], data["dům"], data["pokoje"] = zip(*data['url'].map(characteristics))

    data["podil"] = data["text"].map(ownership_share)
    # print("4/8 Získány informace z URLs, nyní získávám informace z popisu.")
    # print("5/8 Získány informace z Popisu, nyní mapuji Adresy z předešlých inzerátů.")

    # 6 = Adresy z předešlých inzerátů a short_coords
    # data = pd.read_excel(
    #     r"a3_Popisky_prodej_byty.xlsx")  # Abych se vyhnul konverzi TUPLE na STRING, což není triviální, tak si to radši uložím a znova načtu a získám stringy rovnou. Snad mi to nerozbije zbytek
    # data["short_coords"] = data["coords"].apply(short_coords)
    # data_upd = adress_old(
    #     data)  # Tady nepotřebuji maping, protože se nesnažím něco nahodit na všechny řádky, ale merguju celé datasety
    # data = data_upd.copy()
    # print(
    #     "6/8 Namapovány Adresy z předešlých inzerátů, nyní stahuji nové Adresy - několik minut...")  # Přidat do printu počet řádků, kolik mám a kolik zbývá v 7. kroku
    #
    # # 7-8 = Adresy - zbývající přes GeoLocator + Merging
    #
    # try:  # !!! Riskuju že zas něco selže, jako USER- AGENT posledně...
    #     data_upd = adress_merging(
    #         data)  # Přidáno TRY pro situace, kdy už mám všechyn adresy z OLD a nejde nic namapovat !
    #     data = data_upd.copy()
    #     data.to_excel(r"a4_SCRAPED_prodej_byty.xlsx")
    #     print("7+8/8 Získány nové adresy + mergnuto dohromady. Celková délka datasetu: " + str(
    #         len(data)) + ". Konec Fáze 1.")
    #
    # except:
    #     data.to_excel(r"a4_SCRAPED_prodej_byty.xlsx")
    #     print("7+8/8 ŽÁDNÉ nové adresy. Celková délka datasetu: " + str(len(data)) + ". Konec Fáze 1.")

    data.to_excel(f"a4_SCRAPED_{typ_stavby}.xlsx")
    return data


# scrap_all(typ_stavby="byty")
scrap_all(typ_stavby="byty")
