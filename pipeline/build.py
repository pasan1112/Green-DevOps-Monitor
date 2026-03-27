import time

def run_build():
    print("Running build stage...")
    for _ in range(3):
        sum = 0
        for i in range(5_000_000):
            sum += i
        time.sleep(1)