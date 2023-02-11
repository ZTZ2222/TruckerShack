from seleniumwire import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from config import central_login, central_password

def collect_token():
    url = "https://id.centraldispatch.com/Account/Login?ReturnUrl=%2Fconnect%2Fauthorize%2Fcallback%3Fclient_id%3Dcentraldispatch_authentication%26scope%3Dlisting_service%2520offline_access%2520openid%26response_type%3Dcode%26redirect_uri%3Dhttps%253A%252F%252Fwww.centraldispatch.com%252Fprotected"
    options = webdriver.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument("--start-maximized")
    options.add_argument("--headless")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36")
    options.add_experimental_option('excludeSwitches', ['enable-automation'])
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--disable-blink-features=AutomationControlled")
    s = Service(executable_path='/home/ztz/Desktop/MyProjects/Webscraping/Selenium/chromedriver/chromedriver')
    driver = webdriver.Chrome(service=s, options=options)
    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            )
    wait = WebDriverWait(driver, 30)
    try:
        driver.get(url)
        # Log-in
        email_input = wait.until(
            EC.presence_of_element_located((By.ID, "Username"))
        )
        email_input.clear()
        email_input.send_keys(central_login) #Login
        password_input = wait.until(
            EC.presence_of_element_located((By.ID, "password"))
        )
        password_input.clear()
        password_input.send_keys(central_password)
        password_input.send_keys(Keys.RETURN) #Password
        #Access to loadboard
        find_shipments = driver.find_elements(By.CLASS_NAME, "dropdown")[1]
        find_shipments.click()
        search_vehicles = wait.until(
            EC.element_to_be_clickable((By.ID, "navSearchVehicles"))
        )
        search_vehicles.click()
        request = driver.wait_for_request('/api/open-search', timeout=120)
        token_parser = request.headers['authorization']
    except Exception:
        driver.refresh()
        request = driver.wait_for_request('/api/open-search', timeout=120)
        token_parser = request.headers['authorization']
    with open('token.txt', 'w') as f:
        f.write(token_parser)
    f.close()
    driver.close()
    driver.quit()
    return token_parser
