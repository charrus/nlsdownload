#!/usr/bin/python

import json, os.path, httplib2
from tqdm import trange
from multiprocessing import Process, Queue, freeze_support
from PIL import Image
import pprint

def worker(download_queue, montage_queue):
    http = httplib2.Http()

    for tile in iter(download_queue.get, 'STOP'):
        if not os.path.isfile(tile['file_name']):
            resp, tileimage = http.request(tile['url'], 'GET')
            assert resp.status == 200
            with open(tile['file_name'], "wb") as image:
                image.write(tileimage)

        montage_queue.put(tile)

# MAPID = '102898855' # Hammersmith SW
# MAPID = '103028115' # hammersmith all
# MAPID = '102345961' # Middlex XVI
# MAPID = '101460964' # Sommerset XXXIX.SW
# MAPID = '102345976' # Middlesex XXI
# MAPID = '103028115' # TQ2378
# MAPID = '102345858' # London Sheet J
MAPID = '103313069' # London LII OS

NUM_WORKERS = 4

def main():
    parent = MAPID[:5]

    mapjsonurl = 'http://maps.nls.uk/imgsrv/iipsrv.fcgi?iiif=/{parent}/{mapid}.jp2/info.json'.format(
                    parent=parent, mapid=MAPID)

    http = httplib2.Http()
    resp, mapjson = http.request(mapjsonurl, 'GET')
    assert resp.status == 200
    mapinfo = json.loads(mapjson)

    pprint.pprint(mapinfo)

    map_width = mapinfo['width']
    map_height = mapinfo['height']
    tile_width = mapinfo['tiles'][0]['width']
    tile_height = mapinfo['tiles'][0]['height']

    download_queue = Queue()
    montage_queue = Queue()

    total_tiles = 0

    for x in range(0, map_width, tile_width):
        for y in range(0, map_height, tile_height):
            url = 'http://maps.nls.uk/imgsrv/iipsrv.fcgi?iiif=/{parent}/{mapid}.jp2/{x},{y},256,256/pct:100/0/native.jpg'.format(
                    parent=parent, mapid=MAPID, x=x, y=y, tile_width=tile_width, tile_height=tile_height)
            file_name = 'map-{mapid}-{x}-{y}.jpg'.format(mapid=MAPID, x=x, y=y)
            download_queue.put({'url': url, 'file_name': file_name, 'x': x, 'y': y})
            total_tiles += 1

    processes = [Process(target=worker, args=(download_queue, montage_queue))
                 for _ in range(NUM_WORKERS)]

    for proc in processes:
        proc.start()

    montage = Image.new('RGB', (map_width, map_height))

    for _ in trange(total_tiles):
        tile = montage_queue.get()
        tile_im = Image.open(tile['file_name'])
        montage.paste(tile_im, (tile['x'], tile['y']))

    montage.save('{mapid}.jpg'.format(mapid=MAPID))

    for _ in range(NUM_WORKERS):
        download_queue.put('STOP')

if __name__ == '__main__':
    freeze_support()
    main()
