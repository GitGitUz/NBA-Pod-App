# NBA Podcast Predictions: Live + Wins Draft

Live-synced app for four hosts. Standings, awards, side-by-side comparison, and a snake "Wins Draft".
Anyone with the link (and passcode) can edit anything; changes appear for everyone instantly.

## Deploy on Railway
1. Push this folder to a GitHub repo.
2. Railway → New Project → Deploy from GitHub repo. It detects Node and runs `npm start`.
3. **Add a Volume** (service → Settings → Volumes) mounted at `/data`.
4. **Variables** (service → Variables):
   - `DATA_DIR` = `/data`   (so predictions survive redeploys)
   - `PASSCODE` = something only the four of you know (leave unset = anyone with the URL can edit)
5. Settings → Networking → **Generate Domain**. Share that URL.

## Run locally
    npm install
    PASSCODE=hoops npm start      # http://localhost:3000

## Notes
- State lives in `$DATA_DIR/state.json` (one small JSON file). Back it up any time with "Export JSON" in the Side-by-Side tab.
- Run a single instance only (the default). Multiple replicas would not share live state.
- Tailwind loads from its CDN, so each browser needs internet access.
