import sys


def print_data(data):
    with open("players.csv", 'a') as outfile:
        # First print column names
        outfile.write("player,singles,doubles,triples,homeruns,strikes\n")
        for player in data:
            out_line = player
            out_line += "," + str(data[player]['singles'])
            out_line += "," + str(data[player]['doubles'])
            out_line += "," + str(data[player]['triples'])
            out_line += "," + str(data[player]['homeruns'])
            out_line += "," + str(data[player]['strikes'])
            outfile.write(out_line + "\n")
        
    outfile.close()

if __name__ == "__main__":

    results_dict = {}

    with open(sys.argv[1], 'r') as infile:
        for line in infile:
            line_list = line.split("\t")
            player = line_list[0].split("~")[0]
            play = line_list[0].split("~")[1]
            if player not in results_dict:
                results_dict[player] = {}
                results_dict[player]['strikes'] = 0
                results_dict[player]['singles'] = 0
                results_dict[player]['doubles'] = 0
                results_dict[player]['triples'] = 0
                results_dict[player]['homeruns'] = 0
            results_dict[player][play] = line_list[1].rstrip()

    infile.close()
    
    print_data(results_dict)
