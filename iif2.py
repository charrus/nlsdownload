#!/usr/bin/env python

import asyncio
import argparse
import os
import time

import httpx
from aiofile import async_open
from PIL import Image

# from pathlib import Path


QUEUE_SIZE = 8


async def consumer(queue, client):
    while True:
        tile = await queue.get()
        print(f"Requesting: {tile['url']}")
        r = await client.get(tile["url"])
        print(
            f"Got status code: {r.status_code} for {tile['url']} => {tile['file']}"
        )
        if r.status_code == 200:
            async with async_open(tile["file"], mode="wb") as img:
                await img.write(r.content)
        queue.task_done()


def tile_url(base_url, scale, img_type, x, y, width, height):
    return f"{base_url}/{x},{y},{width},{height}/{width},{height}/{scale}/default.{img_type}"


def tile_filename(path, img_type, x, y):
    return f"{path}_{x}_{y}.{img_type}"


async def main():
    parser = argparse.ArgumentParser(description="Download and stitch IIIF tiles")
    parser.add_argument("--url", help="IIIF manifest URL")
    parser.add_argument("--output", help="Output filename")
    args = parser.parse_args()

    # Use provided manifest URL or default to example
    imageurl = (
        args.url
        or "https://map-view.nls.uk/iiif/2/10234%2F102345876/info.json"
    )
    queue = asyncio.Queue()

    path = imageurl.split("/")[-2]
    img_type = "jpg"
    if args.output:
        output_path = args.output
        img_type = output_path.split(".")[-1]
    else:
        output_path = f"{path}.{img_type}"

    async with httpx.AsyncClient(http2=True) as client:
        r = await client.get(imageurl)
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
        tasks = []

        for _ in range(QUEUE_SIZE):
            tasks.append(asyncio.create_task(consumer(queue, client)))

        for x in range(0, image_data["width"], tile_width):
            for y in range(0, image_data["height"], tile_height):
                tile = {"x": x, "y": y}
                # The right-most and bottom-most tiles need to be decreased
                # if they would exceed the size of the full image.
                this_tile_width = min(tile_width, image_data["width"] - x)
                this_tile_height = min(tile_height, image_data["height"] - y)
                tile["file"] = tile_filename(path, img_type, x, y)
                tile["url"] = tile_url(
                    base_url, scale, img_type, x, y, this_tile_width, this_tile_height
                )
                tiles.append(tile)
                queue.put_nowait(tile)

        await queue.join()

        for task in tasks:
            task.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

    print(f"Creating montage {output_path}")
    montage = Image.new("RGB", (image_data["width"], image_data["height"]))
    for tile in tiles:
        with Image.open(tile["file"]) as im:
            montage.paste(im, (tile["x"], tile["y"]))
        os.remove(tile["file"])
    montage.save(output_path)
    print(f"Montage saved to {output_path}")


if __name__ == "__main__":
    started_at = time.monotonic()
    asyncio.run(main())
    total_slept_for = time.monotonic() - started_at
    print(f"{QUEUE_SIZE} workers took {total_slept_for:.2f} seconds")
