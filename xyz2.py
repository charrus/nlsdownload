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
            if response.is_success:
                if not retry:
                    sys.stderr.write("#")  # # = OK
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

    sys.stderr.flush()
    return tile


def get_tiles(tmpdir: Path, image_data: dict) -> iter:
    """Generator for all the tiles in the xyz map dataset
    to download."""

    for x in range(image_data["startx"], image_data["endx"]):
        for y in range(image_data["starty"], image_data["endy"]):
            tile = {
                "x": image_data["tile_width"] * (x - image_data["startx"]),
                "y": image_data["tile_height"] * (y - image_data["starty"]),
                "z": image_data["scale"],
            }

            tile["file"] = Path(
                tmpdir,
                f"{image_data["path"]}_{x}_{y}.tmp",
            )
            tile["url"] = (
                image_data["base_url"]
                .replace("{x}", str(x))
                .replace("{y}", str(y))
                .replace("{z}", str(image_data["scale"]))
            )
            yield tile


async def download_tiles(image_data: dict) -> list:
    """Download IIF tiles and create a montage image."""

    tmpdir = Path("tiles")
    tmpdir.mkdir(exist_ok=True)

    async with httpx.AsyncClient(http2=True) as session:
        tasks = []
        results = []

        for tile in get_tiles(
            tmpdir,
            image_data,
        ):
            tasks.append(asyncio.create_task(fetch_tile(session, tile)))

        # Exit early if there are exceptions
        while tasks:
            done, pending = await asyncio.wait(
                tasks,
                return_when=asyncio.FIRST_EXCEPTION,
            )

            for task in done:
                if exc := task.exception():
                    print(f"Exception raised: {exc}, aborting")
                    return []
                else:
                    results.append(task.result())

            tasks = pending

    return results


async def main(file: str, output_path: str):
    """Download IIF tiles and create a montage image."""

    async with aiofiles.open(file, mode="r") as image_data_file:
        image_data_contents = await image_data_file.read()

        image_data = json.loads(image_data_contents)

        image_data = image_data["data"]["result"][0]
        image_data["base_url"] = image_data["overlays"][0]["overlay"]["url"]
        image_data["path"] = image_data["slug"]
        image_data["startx"] = 261808 - 9
        # startx = 1047234 - 19
        # endx = 1047385
        image_data["endx"] = image_data["startx"] + 55
        # starty = 697468 + 10
        image_data["starty"] = 174377 - 10
        image_data["endy"] = image_data["starty"] + 38
        image_data["tile_width"] = 256
        image_data["tile_height"] = 256
        image_data["scale"] = image_data["overlays"][0]["overlay"]["max_zoom"]

    width = (image_data["endx"] - image_data["startx"]) * image_data["tile_width"]
    height = (image_data["endy"] - image_data["starty"]) * image_data["tile_height"]

    tiles = await download_tiles(image_data)

    if tiles:
        print()
        print(f"Creating montage {output_path}")
        montage = Image.new("RGB", (width, height))
        for tile in tiles:
            with Image.open(tile["file"]) as im:
                montage.paste(im, (tile["x"], tile["y"]))
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
    print(f"workers took {total_slept_for:.2f} seconds")
