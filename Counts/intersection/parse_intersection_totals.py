import argparse
from intersection_counts import read_raw_year_totals

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "intersection_counts_am_filepath", help="Input filepath (a.m.)"
    )
    parser.add_argument(
        "intersection_counts_pm_filepath", help="Input filepath (p.m.)"
    )
    parser.add_argument(
        "parsed_intersection_totals_csv_filepath", help="Output CSV filepath"
    )
    args = parser.parse_args()

    read_raw_year_totals(
        args.intersection_counts_am_filepath,
        args.intersection_counts_pm_filepath,
    ).to_csv(args.parsed_intersection_totals_csv_filepath)
