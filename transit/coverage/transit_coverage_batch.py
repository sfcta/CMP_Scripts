"""Batch run transit_coverage analysis"""
import argparse
import os
import tomllib

from transit_coverage import transit_coverage

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Batch run transit_coverage analysis on all config files in configs_dir."
    )
    parser.add_argument(
        "configs_dir", help="directory filepath to config .toml's"
    )
    args = parser.parse_args()

    for f in os.scandir(args.configs_dir):
        if f.name.endswith(".toml"):
            print("On config file:", f.name, end="... ")
            with open(f.path, "rb") as config_f:
                transit_coverage(tomllib.load(config_f))
            print("Done.")
