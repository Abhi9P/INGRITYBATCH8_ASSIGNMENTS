import multiprocessing
import time

result = []


def cal_squire(numbers):
    # print map(lambda x: x *x, numbers)
    global result
    for n in numbers:
        time.sleep(2)
        print('squire ' + str(n * n))
        result.append(n)


def cal_cube(numbers):
    for n in numbers:
        time.sleep(2)
        print('cube ' + str(n * n * n))


if __name__ == "__main__":
    start = time.time()
    arr = [2, 4, 5, 6, 3]
    t1 = multiprocessing.Process(target=cal_squire, args=(arr,))
    t2 = multiprocessing.Process(target=cal_cube, args=(arr,))
    # cal_squire(arr)
    # cal_cube(arr)
    t1.start()
    t2.start()

    t1.join()
    t2.join()

    end = time.time()

    print(result)
    print(end - start)
