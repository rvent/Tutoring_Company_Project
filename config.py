import getpass
import base64
from cryptography.fernet import Fernet

f = Fernet(Fernet.generate_key())

client_id = input("Enter your yelp client ID goes here:\n")
api_key = input("Enter your yelp API key:\n")

email = "fistudentvent@gmail.com"

def __main__():
    passwd = f.encrypt(getpass.getpass("Password:\n").encode())
    return passwd
