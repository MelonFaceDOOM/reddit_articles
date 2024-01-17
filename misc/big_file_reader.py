import json
import os

"""this method of reading a file was deprecated
instead, simply use parsed_data = json.load(f), for dict in parsed_data... (lmao)"""


def parse_decompressed_file(decompressed_file, output_folder):
    """parses reddit comments or submissions and saves relevant data while only read a chunk of the file at a time."""
    reader = BigFileReader(file_location=decompressed_file, chunk_size=300_000_000)
    decompressed_file_name = decompressed_file.split('/')[-1]
    partitioner = BigJSONPartitioner(output_folder=output_folder,
                                     name_template=decompressed_file_name + "_{file_number}.json",
                                     max_partition_size=2_000_000_000)
    excess = ""
    for chunk in reader.yield_chunks():
        chunk = excess + chunk
        dicts_from_chunk, excess = extract_dicts_from_chunk(chunk)
        #        uncomment next 3 lines to print the first dict
        #        print(dicts_from_chunk[0].keys())
        #        print(dicts_from_chunk[0])
        #        sys.exit()
        partitioner.append_dicts(dicts_from_chunk)

    partitioner.save_partition()  # manually call save_partition() on the final partition since it only automatically saves once max_partition_size is reached.


def extract_dicts_from_chunk(chunk):
    """takes a chunk of text with \n separated dicts, extracts all dicts, and returns excess text at the end"""
    parsed_dicts = []  # TODO: deque might improve performance
    excess = ""
    lines = chunk.split("\n")
    if len(lines) > 1:
        for line in lines[:-1]:
            try:
                parsed_dict = json.loads(line)
                parsed_dicts.append(parsed_dict)
            except Exception as e:
                with open('faulty_lines.txt', 'a+', encoding='utf-8') as f:
                    f.write(line + '\n')
    try:
        # last line will most likely be incomplete and unreadable
        parsed_dict = json.loads(lines[-1])
        parsed_dicts.append(parsed_dict)
    except json.decoder.JSONDecodeError:
        excess = lines[-1]
    return parsed_dicts, excess


class BigFileReader:
    """it turns out this is unneeded because I can just read the file one line at a time,
    whereas this just grabs big arbitrary chunks """

    def __init__(self, file_location, chunk_size, amount_to_read=0, start_position=0, encoding='utf-8'):
        self.file_location = file_location
        self.encoding = encoding
        self.chunk_size = chunk_size
        self.amount_to_read = amount_to_read  # 0 == no limit; full file will be read
        self.amount_read = 0
        self.file = open(file_location, 'r', encoding='utf-8')
        if start_position > 0:
            self.acquire_start_position(start_position)

    def read_chunk(self):
        return self.file.read(self.chunk_size)

    def acquire_start_position(self, amount_to_read_to_reach_start_position):
        """read file up to start position in increments of self.chunk_size"""
        while amount_to_read_to_reach_start_position > 0:
            if amount_to_read_to_reach_start_position > self.chunk_size:
                self.file.read(self.chunk_size)
                amount_to_read_to_reach_start_position -= self.chunk_size
            else:
                self.file.read(amount_to_read_to_reach_start_position)
                amount_to_read_to_reach_start_position -= amount_to_read_to_reach_start_position

    def yield_chunks(self):
        """yields chunks of self.chunk_size up to self.amount_to_read"""
        while True:
            chunk = self.read_chunk()
            self.amount_read += len(chunk)
            if not chunk:
                break  # (i.e. end of file has been reached)
            if self.amount_to_read_exceeded():
                excess_amount = self.amount_read - self.amount_to_read
                chunk = chunk[:-excess_amount]  # truncate excess
                yield chunk
                break
            else:
                yield chunk

    def amount_to_read_exceeded(self):
        if self.amount_to_read == 0:
            return False
        elif self.amount_read < self.amount_to_read:
            return False
        else:
            return True


class BigJSONPartitioner:
    """This is unneeded too because the json library just does everything it does basically"""

    def __init__(self, output_folder, name_template, max_partition_size):
        self.max_partition_size = max_partition_size
        self.dict_list = []  # TODO: deque might improve performance
        self.output_size = 0
        self.file_counter = 0
        self.output_folder = output_folder  # must be absolute path to folder
        self.name_template = name_template  # must be string with named portion for file_number (i.e. "data_{file_number}.json")

    def append_dict(self, new_dict):
        self.dict_list.append(new_dict)
        dict_in_text = json.dumps(new_dict)
        self.output_size += len(dict_in_text)
        if self.output_size > self.max_partition_size:
            self.save_partition()
            self.file_counter += 1

    def append_dicts(self, new_dict_list):
        for new_dict in new_dict_list:
            self.append_dict(new_dict)

    def generate_file_path(self):
        file_name = self.name_template.format(file_number=self.file_counter)
        return os.path.join(self.output_folder, file_name)

    def save_partition(self):
        file_path = self.generate_file_path()
        print(f"length: {len(self.dict_list)} size: {self.output_size} to: {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(json.dumps(self.dict_list))
        self.dict_list = []
        self.output_size = 0


def create_sample_file():
    text_list = []
    for i in range(100):
        sublist = "".join([str(j) for j in range(1000)])
        text_list.append(sublist)
    text_list = "\n".join(text_list)
    with open('temp.txt', 'w') as f:
        f.write(text_list)

