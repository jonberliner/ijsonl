# create_test_files.py
import numpy as np

def create_test_files():
    # Create int8_file.bin
    int8_data = np.arange(1000, dtype=np.int8)
    int8_data.tofile('int8_file.bin')

    # Create str_file.bin
    str_data = b''.join([f"String{i:03d}".encode('utf-8').ljust(10) for i in range(200)])
    with open('str_file.bin', 'wb') as f:
        f.write(str_data)

if __name__ == "__main__":
    create_test_files()
    print("Test files created: int8_file.bin and str_file.bin")

    import subprocess

    def run_rust_program(args):
        command = ["cargo", "build"]
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        command = ["cargo", "run", "--"] + args
        print(result.stdout)
        print(result.stderr)
        try:
            result = subprocess.run(command, check=True, capture_output=True, text=True)
            print(result.stdout)
        except subprocess.CalledProcessError as e:
            print("Error occurred:")
            print(e.stderr)

    # Run Rust program
    run_rust_program(["int8", "int8_file.bin", "0", "100", "int8_file.bin", "200", "300", 
                    "utf8", "str_file.bin", "0", "200"])
