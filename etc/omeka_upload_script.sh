#!/bin/bash
# atom_upload script example
# /etc/archivematica/automation-tools/atom_upload_script.sh
cd /usr/lib/archivematica/automation-tools/
/usr/share/python/automation-tools/venv/bin/python -m dips.omeka_upload \
  --omeka-api-url <url> \
  --omeka-api-key-identity <key> \
  --omeka-api-key-credential <key> \
  --ss-url <url> \
  --ss-user <user> \
  --ss-api-key <key> \
  --dip-uuid <uuid>
