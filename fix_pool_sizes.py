"""Fix bad pool_size values and merge to master branch."""
import sqlite3

conn = sqlite3.connect('aup_dashboard.db')
conn.row_factory = sqlite3.Row

# Check bad pool_size values for CC issuers
CC_ISSUERS = [
    'newday_funding', 'mercury_financial', 'mission_lane', 'continental_finance',
    'genesis_financial', 'avant_card', 'access_financial', 'imprint_payments',
    'fair_square', 'prosper_card', 'cw_nexus',
]
ph = ','.join('?' * len(CC_ISSUERS))
rows = conn.execute(f"""
    SELECT p.id, f.issuer_key, f.filed_date, p.pool_size, p.sample_size
    FROM filings f JOIN procedures p ON p.filing_id = f.id
    WHERE f.issuer_key IN ({ph})
      AND p.pool_size IS NOT NULL AND p.pool_size != ''
    ORDER BY f.issuer_key, f.filed_date DESC
""", CC_ISSUERS).fetchall()

print("Pool sizes for CC issuers:")
bad_count = 0
for r in rows:
    pool = r['pool_size']
    # Pool sizes like '2', '25', '100' are clearly wrong for CC securitizations
    # Real pool sizes would be in thousands; anything under 500 is suspicious
    try:
        pool_int = int(pool)
        if pool_int < 500:
            print(f"  BAD: {r['issuer_key']} {r['filed_date']}: pool={pool!r} sample={r['sample_size']!r}")
            # Clear the bad pool size
            conn.execute('UPDATE procedures SET pool_size=NULL WHERE id=?', (r['id'],))
            bad_count += 1
        else:
            print(f"  OK: {r['issuer_key']} {r['filed_date']}: pool={pool!r}")
    except (ValueError, TypeError):
        print(f"  NON-INT: {r['issuer_key']} {r['filed_date']}: pool={pool!r}")

conn.commit()
print(f"\nCleared {bad_count} bad pool_size values")
conn.close()
