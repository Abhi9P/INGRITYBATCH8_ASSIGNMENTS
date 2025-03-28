import pandas as pd
import random
import multiprocessing


# Generate a large dataset with 1 million rows
def generate_large_dataset():
    """
    Generates a CSV file named 'large_numbers.csv' containing 1 million rows of data.
    Each row has:
    - 'ID': A unique identifier ranging from 1 to 1,000,000
    - 'Number': A randomly generated integer between 1 and 1,000,000
    """
    num_rows = 1_000_000  # Define the number of rows for the dataset
    data = {
        'ID': range(1, num_rows + 1),  # Generate unique IDs from 1 to 1,000,000
        'Number': [random.randint(1, 1_000_000) for _ in range(num_rows)]  # Generate random numbers
    }
    # Save the dataset to a CSV file
    pd.DataFrame(data).to_csv("large_numbers.csv", index=False)
    print("Dataset 'large_numbers.csv' generated successfully.")


def process_chunk(chunk, return_dict, index):
    """
    Processes a chunk of the dataset and calculates:
    - Sum of all numbers in the chunk
    - Maximum and minimum numbers in the chunk
    - Count of even and odd numbers in the chunk

    Args:
        chunk (DataFrame): The chunk of data to process.
        return_dict (Manager.dict): A shared dictionary to store results from multiple processes.
        index (int): The index of the current chunk being processed.
    """
    return_dict[index] = {  # Store results in the shared dictionary using the chunk index as the key
        'sum': chunk['Number'].sum(),
        'max': chunk['Number'].max(),
        'min': chunk['Number'].min(),
        'even': (chunk['Number'] % 2 == 0).sum(),
        'odd': (chunk['Number'] % 2 == 1).sum()
    }


def compute_stats(file_name):
    """
    Reads the CSV file in chunks and computes overall statistics using multiprocessing.
    - Splits the file into chunks for parallel processing.
    - Aggregates the results from each chunk to produce final results.

    Args:
        file_name (str): The path to the CSV file to process.

    Returns:
        tuple: Total sum, maximum number, minimum number, even count, and odd count.
    """
    manager = multiprocessing.Manager()  # Create a multiprocessing manager
    return_dict = manager.dict()  # Create a shared dictionary for storing results from each process
    processes = []  # Store all the process objects

    # Read the file in chunks of approximately 166,666 rows each (1,000,000 / 6 = 166,666)
    for index, chunk in enumerate(pd.read_csv(file_name, chunksize=166_666)):
        # Create a new process for processing each chunk
        p = multiprocessing.Process(target=process_chunk, args=(chunk, return_dict, index))
        processes.append(p)  # Add the process to the list of processes
        p.start()  # Start the process

    # Wait for all processes to complete
    for p in processes:
        p.join()

    # Aggregate results from all chunks
    total_sum = sum(res['sum'] for res in return_dict.values())
    max_number = max(res['max'] for res in return_dict.values())
    min_number = min(res['min'] for res in return_dict.values())
    even_count = sum(res['even'] for res in return_dict.values())
    odd_count = sum(res['odd'] for res in return_dict.values())

    return total_sum, max_number, min_number, even_count, odd_count


if __name__ == "__main__":
    # Step 1: Generate the dataset
    generate_large_dataset()

    # Step 2: Compute statistics on the generated dataset
    total_sum, max_number, min_number, even_count, odd_count = compute_stats("large_numbers.csv")

    # Step 3: Display results
    print(f"\n=== STATISTICS ===")
    print(f"Total Sum of all numbers: {total_sum}")
    print(f"Maximum Number: {max_number}")
    print(f"Minimum Number: {min_number}")
    print(f"Even Numbers Count: {even_count}")
    print(f"Odd Numbers Count: {odd_count}")
