#!/usr/bin/env python
"""IIF Tile Downloader."""

import argparse
import asyncio
import sys
import time
from pathlib import Path

import aiofiles
import httpx
from PIL import Image

QUEUE_SIZE = 16


async def consumer(queue: asyncio.Queue, client: httpx.AsyncClient):
    """Process tiles from the queue."""
    while True:
        tile = await queue.get()
        for retry in range(2):
            r = await client.get(tile.get("url"))
            if r.status_code == 200:
                if retry:
                    sys.stderr.write("*")
                else:
                    sys.stderr.write("#")
                async with aiofiles.open(tile["file"], mode="wb") as img:
                    await img.write(r.content)
                break   # Exit from retry loop
            else:
                sys.stderr.write("!")
                await asyncio.sleep(5)

        sys.stderr.flush()
        queue.task_done()


def tile_url(
    base_url: str,
    scale: int,
    img_type: str,
    x: int,
    y: int,
    width: int,
    height: int,
) -> str:
    """Generate the URL for a tile image."""
    return (
        f"{base_url}/"
        f"{x},{y},{width},{height}/"
        f"{width},{height}/{scale}/default.{img_type}"
    )


def tile_filename(path: str, img_type: str, x: int, y: int) -> str:
    """Generate the filename for a tile image."""
    return f"{path}_{x}_{y}.{img_type}"


async def main(imageurl: str, output_path: str, img_type: str):
    """Download IIF tiles and create a montage image."""
    queue = asyncio.Queue()

    print(f"Downloading tiles for {imageurl}:")
    async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
        async with httpx.AsyncClient(http2=True) as client:
            r = await client.get(imageurl)
            if r.status_code != 200:
                print(f"Error fetching image info: {r.status_code}")
                return
            image_data = r.json()
            base_url = image_data.get("id", image_data.get("@id"))
            path = base_url.split("/")[-1]
            tile_width: int = image_data["tiles"][0]["width"]
            tile_height: int = image_data["tiles"][0]["height"]
            # Pick the smallest scale factor(usually 1x)
            # This is to ensure we get the highest resolution tiles available
            scale_factor: int = min(image_data["tiles"][0]["scaleFactors"])
            # And pass the index of scale, as per the API.
            scale: int = image_data["tiles"][0]["scaleFactors"].index(scale_factor)
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
                    tile["file"] = Path(tmpdir, tile_filename(path, img_type, x, y))
                    tile["url"] = tile_url(
                        base_url,
                        scale,
                        img_type,
                        x,
                        y,
                        this_tile_width,
                        this_tile_height,
                    )
                    tiles.append(tile)
                    queue.put_nowait(tile)

            await queue.join()

            for task in tasks:
                task.cancel()

            await asyncio.gather(*tasks, return_exceptions=True)

        print()
        print(f"Creating montage {output_path}")
        montage = Image.new("RGB", (image_data["width"], image_data["height"]))
        for tile in tiles:
            with Image.open(tile["file"]) as im:
                montage.paste(im, (tile["x"], tile["y"]))
        montage.save(output_path)
        print(f"Montage saved to {output_path}")


if __name__ == "__main__":
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(prog='iif2', usage='%(prog)s [options]')
    parser.add_argument(
        "--url",
        help=(
            "IIIF manifest URL."
        ),
        default=(
            "https://map-view.nls.uk/iiif/2/10234%2F102345876/info.json"
        ),
    )
    parser.add_argument("--output", help="Output filename")
    args = parser.parse_args()
    # Use provided manifest URL or default to example
    imageurl = args.url
    path = imageurl.split("/")[-2]
    img_type = "jpg"
    if args.output:
        output_path = args.output
        img_type = output_path.split(".")[-1]
    else:
        output_path = f"{path}.{img_type}"
    asyncio.run(main(imageurl, output_path, img_type))
    total_slept_for = time.monotonic() - started_at
    print(f"{QUEUE_SIZE} workers took {total_slept_for:.2f} seconds")
