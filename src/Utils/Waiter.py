import random
import time

def wait(min: int = 1, max: int = 15):
    seconds = random.randint(min, max)
    print(f'Waiting {seconds} seconds')
    time.sleep(seconds)