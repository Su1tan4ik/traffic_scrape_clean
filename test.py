import pandas as pd

df = pd.read_csv('data/traffic_latest.csv')
df = df[df['traffic_score'] > 0].sort_values('traffic_score', ascending=False)

print('=== TOP 20 самых загруженных тайлов ===')
print()
for _, r in df.head(20).iterrows():
    street = r['street_name'] if pd.notna(r['street_name']) else 'unknown'
    print(f"Score: {r['traffic_score']:.2f} | {r['congestion_level']:7s} | "
          f"lat={r['lat']:.4f} lon={r['lon']:.4f} | "
          f"heavy={int(r['pixels_heavy'])} moderate={int(r['pixels_moderate'])} | "
          f"{street}")