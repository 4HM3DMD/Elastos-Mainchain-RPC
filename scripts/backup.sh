#!/bin/bash
BACKUP_DIR="/opt/ela-indexer/backups"
TG_TOKEN="PASTE_YOUR_BOT_TOKEN_HERE"
TG_CHAT="PASTE_YOUR_CHAT_ID_HERE"
DB_PASS="PASTE_YOUR_DB_PASSWORD_HERE"

mkdir -p "$BACKUP_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/ela_index_$TIMESTAMP.sql.gz"

PGPASSWORD="$DB_PASS" pg_dump -h 127.0.0.1 -U ela_indexer ela_index | gzip > "$BACKUP_FILE"

if [ $? -eq 0 ]; then
    SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
    echo "[backup] $(date) — success: $BACKUP_FILE ($SIZE)"
    find "$BACKUP_DIR" -name "*.sql.gz" -mtime +7 -delete
else
    echo "[backup] $(date) — FAILED"
    curl -s -X POST "https://api.telegram.org/bot${TG_TOKEN}/sendMessage" \
        -d chat_id="${TG_CHAT}" \
        -d text="🔴 <b>ELA Database Backup FAILED</b>%0ATime: $(date '+%Y-%m-%d %H:%M:%S UTC')" \
        -d parse_mode="HTML" > /dev/null 2>&1
fi
