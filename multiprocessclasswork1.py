import pandas as pd
import random
import multiprocessing

# Generate a large dataset with 1 million rows

def generate_large_dataset():
    num_rows = 1_000_000
    data = {
        'ID': range(1, num_rows + 1),
        'Number': [random.randint(1, 1_000_000) for _ in range(num_rows)]
    }
    pd.DataFrame(data).to_csv("large_numbers.csv", index=False)
    print("Dataset 'large_numbers.csv' generated successfully.")


def process_chunk(chunk, return_dict, index):
    return_dict[index] = {
        'sum': chunk['Number'].sum(),
        'max': chunk['Number'].max(),
        'min': chunk['Number'].min(),
        'even': (chunk['Number'] % 2 == 0).sum(),
        'odd': (chunk['Number'] % 2 == 1).sum()
    }


def compute_stats(file_name):
    manager = multiprocessing.Manager()
    return_dict = manager.dict()
    processes = []

    for index, chunk in enumerate(pd.read_csv(file_name, chunksize=166_666)):  
        p = multiprocessing.Process(target=process_chunk, args=(chunk, return_dict, index))
        processes.append(p)
        p.start()

    for p in processes:
        p.join()

    # Aggregate results
    total_sum = sum(res['sum'] for res in return_dict.values())
    max_number = max(res['max'] for res in return_dict.values())
    min_number = min(res['min'] for res in return_dict.values())
    even_count = sum(res['even'] for res in return_dict.values())
    odd_count = sum(res['odd'] for res in return_dict.values())

    return total_sum, max_number, min_number, even_count, odd_count


if __name__ == "__main__":
    generate_large_dataset()
    total_sum, max_number, min_number, even_count, odd_count = compute_stats("large_numbers.csv")
    print(f"Total Sum of all numbers: {total_sum}")
    print(f"Maximum Number: {max_number}")
    print(f"Minimum Number: {min_number}")
    print(f"Even Numbers Count: {even_count}")
    print(f"Odd Numbers Count: {odd_count}")
