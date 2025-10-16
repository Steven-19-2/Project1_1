# import configparser


# def create_config():
#     config = configparser.ConfigParser()

#     # Add sections and key-value pairs
#     config['General'] = {'debug': True, 'log_level': 'info'}

#     config['Aws'] = {'BUCKET_NAME':'zmx-training-bucketbatch2025'}


#     # Write the configuration to a file
#     with open('config.ini', 'w') as configfile:
#         config.write(configfile)

# def read_config():
#     config = configparser.ConfigParser()
#     config.read('config.ini')
#     return config

# if __name__ == "__main__":
#     create_config()

import configparser
from cryptography.fernet import Fernet
import getpass  # for hidden input

# -------------------- Encryption Helper --------------------
def generate_key():
    """Generate a Fernet key and save it to a file."""
    key = Fernet.generate_key()
    with open("secret.key", "wb") as f:
        f.write(key)
    return key

def load_key():
    """Load the Fernet key from file."""
    return open("secret.key", "rb").read()

def encrypt_value(value, key):
    f = Fernet(key)
    return f.encrypt(value.encode()).decode()

def decrypt_value(enc_value, key):
    f = Fernet(key)
    return f.decrypt(enc_value.encode()).decode()

# -------------------- Config Creation --------------------
def create_config():
    # Generate/load encryption key
    try:
        key = load_key()
    except FileNotFoundError:
        key = generate_key()
        print("Encryption key generated and saved as secret.key")

    config = configparser.ConfigParser()

    # --------- General (constant values) ---------
    config['General'] = {
        'debug': 'True',
        'log_level': 'info'
    }

    # --------- AWS (sensitive values) ---------
    bucket_name = input("Enter AWS bucket name: ").strip()
    aws_access_key_id = getpass.getpass("Enter AWS Access Key ID: ").strip()
    aws_secret_access_key = getpass.getpass("Enter AWS Secret Access Key: ").strip()
    region = input("Enter AWS Region: ").strip()

    
    # Encrypt sensitive values
    config['S3'] = {
        'BUCKET_NAME': bucket_name,
        'AWS_ACCESS_KEY_ID': encrypt_value(aws_access_key_id, key),
        'AWS_SECRET_ACCESS_KEY': encrypt_value(aws_secret_access_key, key),
        'REGION': region
    }



    # Write config to file
    with open('config.ini', 'w') as f:
        config.write(f)

    print("Config file created successfully!")

# -------------------- Read Config --------------------
def read_config():
    key = load_key()
    config = configparser.ConfigParser()
    config.read("config.ini")

    # Decrypt sensitive fields
    if 'S3' in config:
        config['S3']['AWS_ACCESS_KEY_ID'] = decrypt_value(config['S3']['AWS_ACCESS_KEY_ID'], key)
        config['S3']['AWS_SECRET_ACCESS_KEY'] = decrypt_value(config['S3zmx-training-bucketbatch2025']['AWS_SECRET_ACCESS_KEY'], key)

    return config

# -------------------- Main --------------------
if __name__ == "__main__":
    create_config()
