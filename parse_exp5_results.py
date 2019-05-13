import matplotlib.pyplot as plt


def parse(method):
    # method: get/put
    print('------' + method + '------')
    numbers = [0, 1, 2, 4, 6]
    # numbers = [0, 6]
    for number in numbers:
        filename = 'exp5_' + method + '_#nodes_killed_' + str(number) + '.txt'
        with open(filename, 'r') as file:
            line = file.readline()
            ys = eval(line)
            plt.plot(range(len(ys)), ys)
            print(number, round(sum(ys[0:99])/100.0, 2), round(sum(ys[100:199])/100.0, 2))
    plt.show()


parse('get')
parse('put')
