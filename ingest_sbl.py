"""
One-time ingest of Small Business Loan ABS issuers.

Runs the standard aup_updater pipeline restricted to the SBL issuer keys so
existing issuers are not touched / re-parsed.
"""

import aup_updater
from issuers import SBL_ISSUERS

# Restrict the updater to only the new SBL issuers
aup_updater.ISSUERS = dict(SBL_ISSUERS)

if __name__ == "__main__":
    summary = aup_updater.check_for_new_filings()
    print(f"\nIssuers checked: {summary['checked_issuers']}")
    print(f"New filings:     {summary['new_filings']}")
    print(f"Errors:          {summary['errors']}")
    for d in summary["details"]:
        print(f"  {d['issuer_key']:16s} {d['filed_date']}  procs={d['procedures']}  provider={d['aup_provider']}")
