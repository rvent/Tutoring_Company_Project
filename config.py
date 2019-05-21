import getpass
import base64
from cryptography.fernet import Fernet

f = Fernet(Fernet.generate_key())


client_id = '5onuGByRauHpN8oN0PEgtw' #Your client ID goes here (as a string)
api_key = 'Ystbu1NrD08vSyMfFl5XxExXnikbKXwv-MtJRUHCSQNjTYWTSpsn7SRkLQUoVE9nf1nr_JCD7oD9kCjQIgMGQbxKFF-HwpB0dresyDzXqx-nztbSq4rhrVq0MHJcXHYx'

email = "fistudentvent@gmail.com"

def __main__():
    passwd = f.encrypt(getpass.getpass("Password:\n").encode())
    return passwd
        