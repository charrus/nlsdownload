#!/usr/bin/env python
"""IIF Tile Downloader."""

import argparse
import asyncio
import json
import sys
import time
from pathlib import Path

import aiofiles
import httpx
from PIL import Image

QUEUE_SIZE = 16


# Simple approach - no queue - just request with retry logic
async def fetch_tile(session, tile):
    """Request the tiles data and save it to the filename.
    If request is unsuccessful try up to 2 times with a
    short pause."""

    if tile["file"].exists() and tile["file"].stat().st_size > 0:
        sys.stderr.write("-")  # Skipped
    else:
        retry = 0
        while retry <= 2:
            response = await session.get(tile.get("url"))
            if response.status_code == 200:
                if not retry:
                    sys.stderr.write("#")  # Success without retry
                else:
                    sys.stderr.write("*")  # * = Need to retry URL
                async with aiofiles.open(tile["file"], mode="wb") as img_file:
                    await img_file.write(response.content)
                break  # From while loop
            else:
                retry += 1
                await asyncio.sleep(5)
        else:
            sys.stderr.write("!")  # Couldn't download tile
            return

    return tile


# Queue based approach - pull off queue tiles to download with retry logic
async def consumer(queue: asyncio.Queue, client: httpx.AsyncClient):
    """Process tiles from the queue."""
    while True:
        tile = await queue.get()
        if not tile["file"].exists():
            for retry in range(2):
                r = await client.get(tile.get("url"))
                if r.status_code == 200:
                    if retry:
                        sys.stderr.write("*")
                    else:
                        sys.stderr.write("#")
                async with aiofiles.open(tile["file"], mode="wb") as img:
                    await img.write(r.content)
                break  # Exit from retry loop
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


async def download_tiles(session, image_data):
    """Download IIF tiles and create a montage image."""

    tasks = []

    for x in range(image_data["startx"], image_data["endx"]):
        for y in range(image_data["starty"], image_data["endy"]):
            tile = {
                "x": image_data["tile_width"] * (x - image_data["startx"]),
                "y": image_data["tile_height"] * (y - image_data["starty"]),
                "z": image_data["scale"],
            }
            tile["file"] = Path(
                image_data["tmpdir"],
                tile_filename(image_data["path"], image_data["img_type"], x, y),
            )
            tile["url"] = (
                image_data["base_url"]
                .replace("{x}", str(x))
                .replace("{y}", str(y))
                .replace("{z}", str(image_data["scale"]))
            )
            # tiles.append(tile)
            tasks.append(fetch_tile(session, tile))

    # tasks = [fetch_tile(client, tile) for tile in tiles]

    downloaded_tiles = await asyncio.gather(*tasks, return_exceptions=True)

    return downloaded_tiles


async def main(file: str, output_path: str):
    """Download IIF tiles and create a montage image."""

    tmpdir = Path("tiles")
    tmpdir.mkdir(exist_ok=True)

    async with httpx.AsyncClient(http2=True) as client:
        async with aiofiles.open(file, mode="r") as image_data_file:
            image_data_contents = await image_data_file.read()

            image_data = json.loads(image_data_contents)
            image_data = image_data["data"]["result"][0]
            image_data["tmpdir"] = tmpdir
            image_data["base_url"] = image_data["overlays"][0]["overlay"]["url"]
            image_data["path"] = image_data["slug"]
            image_data["startx"] = 261808 - 9
            # startx = 1047234 - 19
            # endx = 1047385
            image_data["endx"] = image_data["startx"] + 55
            # starty = 697468 + 10
            image_data["starty"] = 174377 - 10
            image_data["endy"] = image_data["starty"] + 38
            image_data["width"] = (image_data["endx"] - image_data["startx"]) * 256
            image_data["height"] = (image_data["endy"] - image_data["starty"]) * 256
            image_data["tile_width"] = 256
            image_data["tile_height"] = 256
            image_data["scale"] = image_data["overlays"][0]["overlay"]["max_zoom"]
            image_data["img_type"] = image_data["base_url"].split(".")[-1]

            tiles = await download_tiles(client, image_data)

    print()
    print(f"Creating montage {output_path}")
    montage = Image.new("RGB", (image_data["width"], image_data["height"]))
    for tile in tiles:
        if tile:
            try:
                with Image.open(tile["file"]) as im:
                    montage.paste(im, (tile["x"], tile["y"]))
            except Exception as e:
                print(f"Error processing {tile['file']}: {e}")
    montage.save(output_path)
    print(f"Montage saved to {output_path}")


if __name__ == "__main__":
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(prog="iif2", usage="%(prog)s [options]")
    parser.add_argument(
        "--xyz",
        help=("XYZ Json"),
        default=("1940s.json"),
    )
    parser.add_argument("--output", help="Output filename")
    args = parser.parse_args()
    # Use provided manifest URL or default to example
    xyzfile = args.xyz
    if args.output:
        output_path = args.output
    else:
        output_path = "output.jpg"
    asyncio.run(main(xyzfile, output_path))
    total_slept_for = time.monotonic() - started_at
    print(f"{QUEUE_SIZE} workers took {total_slept_for:.2f} seconds")
