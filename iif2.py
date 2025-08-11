#!/usr/bin/env python

import asyncio
from aiofile import async_open
#from pathlib import Path

import httpx
from PIL import Image

QUEUE_SIZE = 8

async def consumer(queue):
    async with httpx.AsyncClient(http2=True) as client:
        while True:
            tile = await queue.get()
            if tile is None:
                break
            print(f"got job: {tile['url']}, {tile['file']}")
            r = await client.get(tile['url'])
            print(f"Got status code: {r.status_code}")
            if r.status_code == 200:
                async with async_open(tile['file'], mode="wb") as img:
                    await img.write(r.content)


async def producer(queue, tiles):
    async def get_tile(tile):
        await queue.put(tile)

    await asyncio.gather(*(get_tile(tile) for tile in tiles))
    for _ in range(len(tiles)):
        await queue.put(None) # Sentinels for consumers to terminate


async def main():
    queue = asyncio.Queue()

    # Use provided manifest URL or default to example
    imageurl = "https://map-view.nls.uk/iiif/2/10234%2F102345976/info.json"

    r = httpx.get(imageurl)
    if r.status_code != 200:
        print(f"Error fetching image info: {r.status_code}")
        return
    image_data = r.json()
    base_url = image_data.get("id", image_data.get("@id"))
    path = base_url.split("/")[-1]
    tile_width = image_data["tiles"][0]["width"]
    tile_height = image_data["tiles"][0]["height"]
    # Pick the smallest scale factor(usually 1x)
    # This is to ensure we get the highest resolution tiles available
    scale_factor = min(image_data["tiles"][0]["scaleFactors"])
    # And pass the index of scale, as per the API.
    scale = image_data["tiles"][0]["scaleFactors"].index(scale_factor)
    tiles = []
    for x in range(0, image_data['width'], tile_width):
        for y in range(0, image_data['height'], tile_height):
            tile = {'x': x, 'y': y}
            # The right-most and bottom-most tiles need to be decreased
            # if they would exceed the size of the full image.
            tile['width'] = min(tile_width, image_data['width'] - x)
            tile['height'] = min(tile_height, image_data['height'] - y)
            tile['file'] = f"{path}_{x}_{y}.jpg"
            tile['url'] = (
                    f"{base_url}/{x},{y},{tile_width},"
                    f"{tile_height}/{tile_width},{tile_height}/"
                    f"{scale}/default.jpg"
            )
            tiles.append(tile);

    await asyncio.gather(
            producer(queue, tiles),
            *(consumer(queue) for _ in range(QUEUE_SIZE)),
            )

    montage = Image.new("RGB", (image_data['width'], image_data['height']))
    for tile in tiles:
        print(f"montage.paste({tile['file']}, {tile['x']}, {tile['y']}")
        with Image.open(tile['file']) as im:
            montage.paste(im, (tile['x'], tile['y']))
    output_path = f"{path}.jpg"
    montage.save(output_path)
    print(f"Montage saved to {output_path}")


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
