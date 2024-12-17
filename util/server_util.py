import argparse
import configparser
import os


def get_config():
    parser = argparse.ArgumentParser(description="Load configuration for different server")
    parser.add_argument('--config', type=str, required=True, help='version of the config file')
    args = parser.parse_args()
    current_dir = os.path.dirname(os.path.abspath(__file__))
    server_ini = os.path.join(current_dir, f'../conf/server_config{args.config}.ini')
    config = configparser.ConfigParser()
    config.read(server_ini, encoding='utf-8')
    return config

# def get_config():
#     current_dir = os.path.dirname(os.path.abspath(__file__))
#     server_ini = os.path.join(current_dir, '../conf/server_config.ini')
#     config = configparser.ConfigParser()
#     config.read(server_ini, encoding='utf-8')
#     return config
