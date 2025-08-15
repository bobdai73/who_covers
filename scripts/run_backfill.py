import argparse
import subprocess
import sys

YEARS = list(range(2016, 2025))  # 2016..2024 inclusive

def sh(cmd):
    print(">>", " ".join(cmd))
    res = subprocess.run(cmd)
    if res.returncode != 0:
        sys.exit(res.returncode)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--season", default="regular", choices=["regular","postseason","both"])
    ap.add_argument("--with-lines", action="store_true")
    args = ap.parse_args()

    for yr in YEARS:
        # fetch
        sh([sys.executable, "scripts/fetch_games.py", "--year", str(yr), "--season", args.season])
        sh([sys.executable, "scripts/fetch_basic_stats.py", "--year", str(yr), "--season", args.season])
        sh([sys.executable, "scripts/fetch_advanced_stats.py", "--year", str(yr), "--season", args.season])
        if args.with_lines:
            sh([sys.executable, "scripts/fetch_lines.py", "--year", str(yr), "--season", args.season])

        # build (with gzip)
        build_cmd = [sys.executable, "scripts/build_dataset.py", "--year", str(yr), "--season", args.season, "--csv-gz"]
        if args.with_lines:
            build_cmd.append("--with-lines")
        sh(build_cmd)

    print("Backfill complete.")

if __name__ == "__main__":
    main()
