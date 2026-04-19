#!/usr/bin/env bash
# Replit start script: install deps on first run, then launch the app.
set -e

# Install Python packages if not already cached
if [ ! -d ".pythonlibs" ]; then
    echo "Installing Python dependencies..."
    pip install --user -r requirements.txt
fi

# Create runtime directories
mkdir -p /tmp/retail-shift-sessions /tmp/retail-shift-uploads

# Run Flask — gunicorn in production, flask dev server otherwise
if [ "${REPL_DEPLOYMENT}" = "1" ]; then
    exec gunicorn --bind 0.0.0.0:${PORT:-8080} --workers 2 --timeout 120 app:app
else
    exec python app.py
fi
