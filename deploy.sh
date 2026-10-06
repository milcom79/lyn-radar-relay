#!/usr/bin/env bash
# Oppdaterer relayet på Hostinger-VPS-en (lyn.kartradar.no) til siste versjon
# fra GitHub. Kjøres på serveren som root:  /opt/lyn-radar-relay/deploy.sh
set -euo pipefail
cd /opt/lyn-radar-relay
sudo -u lynrelay git pull --ff-only
sudo -u lynrelay npm ci --omit=dev
systemctl restart lyn-radar-relay
sleep 2
systemctl --no-pager --lines=5 status lyn-radar-relay
