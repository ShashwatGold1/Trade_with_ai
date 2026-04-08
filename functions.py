import json

def load_config():
    with open(r'C:\Users\ojhas\OneDrive\Desktop\Trade_with_ai\Data\config.json', 'r') as f:
        config = json.load(f)
    return config
