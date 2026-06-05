Import transactions into YNAB from Handelsbanken and Spendwise from xlsx files.

## Usage

```bash
ynab-import ~/Downloads/Handelsbanken.xlsx
ynab-import ~/Downloads/Handelsbanken.xlsx --dry-run
ynab-import ~/Downloads/Handelsbanken.xlsx --since 2026-01-01 --auto-confirm
```

Or run directly without the wrapper:

```bash
~/Code/Erik/ynab-import/bin/python ~/Code/Erik/ynab-import/main.py ~/Downloads/Handelsbanken.xlsx
```

## Setup

### 1. Clone and install dependencies

```bash
git clone https://github.com/Yeetii/ynab-import.git ~/Code/Erik/ynab-import
cd ~/Code/Erik/ynab-import
python3 -m venv .
pip install -r requirements.txt
```

### 2. Configure

Copy `.env.example` to `.env` and fill in your YNAB API key and budget/account IDs.

### 3. Install the `ynab-import` command

```bash
mkdir -p ~/.local/bin
cat > ~/.local/bin/ynab-import << 'EOF'
#!/bin/bash
REPO=~/Code/Erik/ynab-import
"$REPO/bin/python" "$REPO/main.py" "$@"
EOF
chmod +x ~/.local/bin/ynab-import
```

Make sure `~/.local/bin` is on your PATH. Add this to `~/.zshrc` if needed:

```bash
export PATH="$HOME/.local/bin:$PATH"
```
