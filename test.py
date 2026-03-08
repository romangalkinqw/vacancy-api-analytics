import requests
from PIL import Image
from io import BytesIO

r = requests.get('https://api.github.com/events')
i = Image.open(BytesIO(r.content))

i.show()