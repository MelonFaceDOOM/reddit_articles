import zstandard
import pathlib


def main():
    input_file = "/data/reddit_data/submissions/compressed/RS_2021-01.zst"
    destination_dir = "/data/reddit_data/submissions/decompressed"
    decompress_zstandard_to_folder(input_file, destination_dir)
    

def decompress_zstandard_to_folder(input_file, destination_dir):
    input_file = pathlib.Path(input_file)
    with open(input_file, 'rb') as compressed:
        decomp = zstandard.ZstdDecompressor(max_window_size=2147483648)
        output_path = pathlib.Path(destination_dir) / input_file.stem
        with open(output_path, 'wb') as destination:
            decomp.copy_stream(compressed, destination)
                

if __name__ == "__main__":
    main()

