#!/usr/bin/env python3
import subprocess
import sys
import os

def run(cmd):
    print(f"> {cmd}")
    subprocess.run(cmd, shell=True, check=True)

if __name__ == "__main__":
    if os.path.exists("./venv"):
        print("Virtual environment already exists. delete venv folder to redeploy.")
        sys.exit(0)
    print("Deploying virtual environment with Python 3.13...")

    # Force Python 3.13
    run("python3.13 -m venv .venv")

    # Activation differs per OS
    if os.name == "nt":  # Windows
        activate = ".venv\\Scripts\\activate.bat"
        pip_cmd = ".venv\\Scripts\\pip.exe"
    else:  # Linux/macOS
        activate = "source .venv/bin/activate"
        pip_cmd = ".venv/bin/pip"

    # Upgrade pip and install requirements
    if os.name == "nt":
        run(".venv\\Scripts\\python.exe -m pip install --upgrade pip")
        run(".venv\\Scripts\\pip.exe install -r requirements.txt")
    else:
        run(f"{activate} && python -m pip install --upgrade pip && pip install -r requirements.txt")

    print("Done!")
