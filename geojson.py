#!/usr/bin/env python
"""IIF Tile Downloader."""

import argparse
import asyncio
import json
import re
import sys
import time
from pathlib import Path

import aiofiles
import geopandas as gpd
import httpx
import numpy as np
import pandas as pd
from PIL import Image
from shapely.geometry import LineString, Point, Polygon

QUEUE_SIZE = 16

POLYGON = '''
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {},
      "geometry": {
        "coordinates": [
          [
            [
              -0.23338715494861617,
              51.49531268692931
            ],
            [
              -0.23338715494861617,
              51.46487551325575
            ],
            [
              -0.19156657736763805,
              51.46487551325575
            ],
            [
              -0.19156657736763805,
              51.49531268692931
            ],
            [
              -0.23338715494861617,
              51.49531268692931
            ]
          ]
        ],
        "type": "Polygon"
      },
      "id": 0
    }
  ],
  "totalFeatures": 1,
  "numberMatched": 1,
  "numberReturned": 1,
  "timeStamp": "2025-08-23T07:13:38.282Z",
  "crs": {
    "type": "name",
    "properties": {
      "name": "urn:ogc:def:crs:EPSG::4326"
    }
  }
}
'''


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


async def main(geojson: Path, output_path: Path):
    """Download IIF tiles and create a montage image."""
    queue = asyncio.Queue()

    mapsdir = Path("maps")
    mapsdir.mkdir(exist_ok=True)
    tmpdir = mapsdir / "tiles"
    tmpdir.mkdir(exist_ok=True)
    img_type = output_path.suffix
    maps = []

    gdf = gpd.read_file(geojson)
    polygon = gdf.from_file("polygon.geojson")
    overlay = gdf.overlay(polygon, how="intersection")
    pattern = r'https://.+info\.json'
    for series in overlay.iterfeatures():
        # ow = overlay.iloc[series]
        properties = series.get("properties")
        geometry = series.get("geometry")
        minx = min(
            [
                geometry["coordinates"][0][i][0]
                for i in range(len(geometry["coordinates"][0]))
            ]
        )
        maxx = max(
            [
                geometry["coordinates"][0][i][0]
                for i in range(len(geometry["coordinates"][0]))
            ]
        )
        miny = min(
            [
                geometry["coordinates"][0][i][1]
                for i in range(len(geometry["coordinates"][0]))
            ]
        )
        maxy = max(
            [
                geometry["coordinates"][0][i][1]
                for i in range(len(geometry["coordinates"][0]))
            ]
        )
        viewerurl = properties["IMAGEURL"]
        filebase = (
            mapsdir
            / f"{properties['id'].split('_WFS')[0]}_{minx:.06f}_{miny:.06f}_{maxx:.06f}_{maxy:.06f}"
        )
        filename = Path(f"{filebase}{img_type}")
        infofile = Path(f"{filebase}.json")
        map = {"filename": filename, "geometry": geometry}
        maps.append(map)

        print(f"Processing {viewerurl}")
        r = httpx.get(viewerurl)
        imageurl = re.findall(pattern, r.text)[0]
        print(f"Got imageurl: {imageurl}")
        infojson = httpx.get(imageurl).json()
        with open(infofile, "w") as p:
            p.write(json.dumps(infojson))
        if filename.exists():
            print(f"Skipping existing {filename}")
            continue
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
            print(f"Creating montage {filename}")
            montage = Image.new("RGB", (image_data["width"], image_data["height"]))
            for tile in tiles:
                with Image.open(tile["file"]) as im:
                    montage.paste(im, (tile["x"], tile["y"]))
            montage.save(filename)
            print(f"Montage saved to {filename}")


if __name__ == "__main__":
    started_at = time.monotonic()
    parser = argparse.ArgumentParser(prog="geojson", usage="%(prog)s [options]")
    parser.add_argument(
        "--geojson",
        help=("GEOJson File"),
        default=("geojson.json"),
    )
    parser.add_argument("--output", help="Output filename")
    args = parser.parse_args()
    # Use provided manifest URL or default to example
    geojson = Path(args.geojson)
    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(f"{geojson}.jpg")
    asyncio.run(main(geojson, output_path))
    total_slept_for = time.monotonic() - started_at
    print(f"{QUEUE_SIZE} workers took {total_slept_for:.2f} seconds")
