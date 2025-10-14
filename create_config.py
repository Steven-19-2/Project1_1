import configparser


def create_config():
    config = configparser.ConfigParser()

    # Add sections and key-value pairs
    config['General'] = {'debug': True, 'log_level': 'info'}

    config['Aws'] = {'BUCKET_NAME':'zmx-training-bucketbatch2025'}


    # Write the configuration to a file
    with open('config.ini', 'w') as configfile:
        config.write(configfile)

def read_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

if __name__ == "__main__":
    create_config()