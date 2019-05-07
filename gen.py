import string
from random import choice, randint

for i in range(30000, 100000):
    str_len = randint(2000, 5000)
    chars = string.ascii_uppercase
    new_str = ''.join([choice(chars) for _ in range(str_len)])
    with open(f'samples/{i}.txt', 'w') as f:
        f.write(new_str)
