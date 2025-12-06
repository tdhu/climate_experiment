"""Headless Doha exceedance run using live station + CMIP6 data.

This script is intentionally lightweight so SMEs can run it without tweaking code.
"""

from pprint import pprint

from climate_analysis.workflows import analyze_site_exceedance


def main():
    results = analyze_site_exceedance("QAT", thresholds_c=[48.0], model="CanESM5")
    print("=== Exceedance results (return times in years) ===")
    for row in results["exceedance_results"]:
        pprint(row.__dict__)
    print("\n=== HRW ===")
    pprint(results["hrw"])
    print("\nHRD sample (head):")
    print(results["hrd_series"].head())


if __name__ == "__main__":
    main()
