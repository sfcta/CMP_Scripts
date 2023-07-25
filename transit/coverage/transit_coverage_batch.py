"""Batch run transit_coverage analysis"""
import argparse
import configparser
import os
from transit_coverage import transit_coverage


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Batch run transit_coverage analysis on all config files in configs_dir.')
    parser.add_argument('configs_dir', help="directory filepath to config .ini's")
    args = parser.parse_args()

    for f in os.scandir(args.configs_dir):
        if f.name.endswith(".ini"):
            config = configparser.ConfigParser()
            config.read(f.path)
            print("On config file:", f.name, end="... ")
            transit_coverage(config)
            print("Done.")