#!/bin/bash
# deploy.sh — Linux/macOS/WSL

set -e  # stop on any error

VENV_DIR="venv"

echo "Creating virtual environment with Python 3.13..."
python3.13 -m venv "$VENV_DIR"

echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

echo "Upgrading pip..."
pip install --upgrade pip

echo "Installing requirements.txt..."
pip install -r requirements.txt

echo ""
echo "All done! Virtual environment is ready."
echo "To activate it later, run:"
echo "   source $VENV_DIR/bin/activate"