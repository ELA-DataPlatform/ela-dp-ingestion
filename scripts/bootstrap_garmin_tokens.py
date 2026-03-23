"""
Bootstrap Garmin tokens locally.

Run once from a residential/office IP to avoid 429 rate limits,
then upload the generated file to GCS:

    python scripts/bootstrap_garmin_tokens.py
    gcloud storage cp garmin_tokens.json gs://ela-source-dev/garmin/tokens/garmin_tokens.json
"""

import os
import sys

from garminconnect import Garmin

OUTPUT_FILE = "garmin_tokens.json"


def main():
    username = os.environ.get("GARMIN_USERNAME")
    password = os.environ.get("GARMIN_PASSWORD")

    if not username or not password:
        print("Error: GARMIN_USERNAME and GARMIN_PASSWORD must be set")
        sys.exit(1)

    print(f"Logging in as {username}...")
    garmin = Garmin(username, password)
    garmin.login()
    garmin.garth.dump(OUTPUT_FILE)
    print(f"Tokens saved to {OUTPUT_FILE}")
    print()
    print("Upload to GCS with:")
    print(f"  gcloud storage cp {OUTPUT_FILE} gs://ela-source-dev/garmin/tokens/garmin_tokens.json")


if __name__ == "__main__":
    main()
