import configparser

def read_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

if __name__ == "__main__":
    config_data = read_config()
    print("Bucket Name:", config_data['Aws']['bucket_name'])
