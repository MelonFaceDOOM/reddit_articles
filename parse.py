import json


def parse(decompressed_file):
    with open(decompressed_file, 'r', encoding='utf-8') as json_file:
        for line in json_file:
            line = line.strip()
            if line:
                data = json.loads(line)
                yield data


def parse_chunks(decompressed_file, chunksize, total=-1):
    """total specifies how many characters to read
       total=-1 will read the whole file"""
    counter = 0
    chunk = []
    for data in parse(decompressed_file):
        chunk.append(data)
        if len(chunk) == chunksize:
            yield chunk
            chunk = []
        counter += 1
        if counter == total:
            break
    if chunk:
        yield chunk
